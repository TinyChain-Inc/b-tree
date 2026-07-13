use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;

use collate::{Collate, Overlap, OverlapsValue};
#[cfg(feature = "stream")]
use destream::{de, en};
use get_size::GetSize;
use uuid::Uuid;

use super::Collator;
use super::range::Range;

const UUID_SIZE: usize = 16;

/// An ordered set of keys in a [`Node`].
pub trait Block<V> {
    type Key;

    fn bisect<C, BV>(&self, range: &Range<BV>, collator: &Collator<C>) -> (usize, usize)
    where
        C: Collate<Value = V>,
        BV: Borrow<V>;

    fn bisect_left<C, BV>(&self, key: &[BV], collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>,
        BV: Borrow<V>;

    fn bisect_right<C, BV>(&self, key: &[BV], collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>,
        BV: Borrow<V>;
}

impl<V: fmt::Debug> Block<V> for Vec<Vec<V>> {
    type Key = Vec<V>;

    fn bisect<C, BV>(&self, range: &Range<BV>, collator: &Collator<C>) -> (usize, usize)
    where
        C: Collate<Value = V>,
        BV: Borrow<V>,
    {
        // handle common edge cases

        if range.is_empty() {
            return (0, self.len());
        }

        if let Some(first) = self.first() {
            if range.overlaps_value(first, collator) == Overlap::Less {
                return (0, 0);
            }
        }

        if let Some(last) = self.last() {
            if range.overlaps_value(last, collator) == Overlap::Greater {
                return (self.len(), self.len());
            }
        }

        // bisect range left
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match range.overlaps_value(&self[mid], collator) {
                Overlap::Greater => lo = mid + 1,
                _ => hi = mid,
            }
        }

        let left = lo;

        // bisect range right
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match range.overlaps_value(&self[mid], collator) {
                Overlap::Less => hi = mid,
                _ => lo = mid + 1,
            }
        }

        let right = hi;

        // return
        (left, right)
    }

    fn bisect_left<C, BV>(&self, key: &[BV], collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>,
        BV: Borrow<V>,
    {
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match collator.cmp_slices(&self[mid], key) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        lo
    }

    fn bisect_right<C, BV>(&self, key: &[BV], collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>,
        BV: Borrow<V>,
    {
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match collator.cmp_slices(&self[mid], key) {
                Ordering::Greater => hi = mid,
                _ => lo = mid + 1,
            }
        }

        hi
    }
}

/// A node in a B+Tree
#[derive(Clone)]
pub enum Node<N> {
    Index(N, Vec<Uuid>),
    Leaf(N),
}

impl<N> Node<N> {
    /// Return `true` if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf(_))
    }
}

impl<N: GetSize> GetSize for Node<N> {
    fn get_size(&self) -> usize {
        match self {
            Self::Index(keys, children) => keys.get_size() + (children.len() * UUID_SIZE),
            Self::Leaf(leaf) => leaf.get_size(),
        }
    }
}

#[cfg(feature = "stream")]
struct NodeVisitor<C, N> {
    context: C,
    phantom: std::marker::PhantomData<N>,
}

#[cfg(feature = "stream")]
impl<C, N> NodeVisitor<C, N> {
    fn new(context: C) -> Self {
        Self {
            context,
            phantom: std::marker::PhantomData,
        }
    }
}

#[cfg(feature = "stream")]
impl<N> de::Visitor for NodeVisitor<N::Context, N>
where
    N: de::FromStream,
{
    type Value = Node<N>;

    fn expecting() -> &'static str {
        "a B+Tree node"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let leaf = seq.expect_next::<bool>(()).await?;

        if leaf {
            match seq.expect_next(self.context).await {
                Ok(leaf) => Ok(Node::Leaf(leaf)),
                Err(cause) => Err(cause),
            }
        } else {
            let bounds = seq.expect_next(self.context).await?;
            let children = seq.expect_next(()).await?;
            Ok(Node::Index(bounds, children))
        }
    }
}

#[cfg(feature = "stream")]
impl<N> de::FromStream for Node<N>
where
    N: de::FromStream,
{
    type Context = N::Context;

    async fn from_stream<D: de::Decoder>(
        cxt: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        decoder.decode_seq(NodeVisitor::new(cxt)).await
    }
}

#[cfg(feature = "stream")]
impl<'en, N: 'en> en::IntoStream<'en> for Node<N>
where
    N: en::IntoStream<'en>,
{
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Node::Leaf(keys) => (true, keys).into_stream(encoder),
            Node::Index(bounds, children) => (false, bounds, children).into_stream(encoder),
        }
    }
}

#[cfg(feature = "stream")]
impl<'en, N: 'en> en::ToStream<'en> for Node<N>
where
    N: en::ToStream<'en>,
{
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        use en::IntoStream;

        match self {
            Node::Leaf(keys) => (true, keys).into_stream(encoder),
            Node::Index(bounds, children) => (false, bounds, children).into_stream(encoder),
        }
    }
}

#[cfg(debug_assertions)]
impl<N: fmt::Debug> fmt::Debug for Node<N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Leaf(keys) => write!(f, "leaf node with keys {:?}", keys),
            Self::Index(bounds, children) => write!(
                f,
                "index node with bounds {:?} and children {:?}",
                bounds, children
            ),
        }
    }
}

#[cfg(not(debug_assertions))]
impl<V: fmt::Debug> fmt::Debug for Node<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Leaf(_keys) => write!(f, "a leaf node"),
            Self::Index(_bounds, children) => {
                write!(f, "an index node with {} children", children.len())
            }
        }
    }
}
