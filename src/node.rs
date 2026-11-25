use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;

use collate::{Collate, Overlap, OverlapsValue};
#[cfg(feature = "stream")]
use destream::de::FromStream;
#[cfg(feature = "stream")]
use destream::{de, en};
use get_size::GetSize;
use uuid::Uuid;

use super::{RangeCollator, range::Range};

/// An ordered set of keys in a [`Node`].
pub trait Block<V> {
    type Key;

    fn bisect<C, BV>(&self, range: &Range<BV>, collator: &RangeCollator<C>) -> (usize, usize)
    where
        C: Collate<Value = V>,
        BV: Borrow<V>;

    fn bisect_left<C, BV>(&self, key: &[BV], collator: &RangeCollator<C>) -> usize
    where
        C: Collate<Value = V>,
        BV: Borrow<V>;

    fn bisect_right<C, BV>(&self, key: &[BV], collator: &RangeCollator<C>) -> usize
    where
        C: Collate<Value = V>,
        BV: Borrow<V>;
}

impl<V: fmt::Debug> Block<V> for Vec<Vec<V>> {
    type Key = Vec<V>;

    fn bisect<C, BV>(&self, range: &Range<BV>, collator: &RangeCollator<C>) -> (usize, usize)
    where
        C: Collate<Value = V>,
        BV: Borrow<V>,
    {
        // handle common edge cases

        if range.is_empty() {
            return (0, self.len());
        }

        match self.first() {
            Some(first) if range.overlaps_value(first, collator) == Overlap::Less => {
                return (0, 0);
            }
            _ => {}
        }

        match self.last() {
            Some(last) if range.overlaps_value(last, collator) == Overlap::Greater => {
                return (self.len(), self.len());
            }
            _ => {}
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

    fn bisect_left<C, BV>(&self, key: &[BV], collator: &RangeCollator<C>) -> usize
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

    fn bisect_right<C, BV>(&self, key: &[BV], collator: &RangeCollator<C>) -> usize
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
    /// Routing separators followed by children; there is one more child than
    /// separator. Leaf values remain complete rows.
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
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Index(keys, children) => {
                keys.get_heap_size() + children.capacity() * std::mem::size_of::<Uuid>()
            }
            Self::Leaf(leaf) => leaf.get_heap_size(),
        }
    }
}

#[cfg(feature = "stream")]
impl<R> Node<Vec<R>> {
    /// Encode this node with a caller-provided row codec, checking index fanout.
    pub fn encode_bounded<'en, E, Rows>(
        &'en self,
        encoder: E,
        max_children: usize,
        rows: impl FnOnce(&'en [R]) -> Rows,
    ) -> Result<E::Ok, E::Error>
    where
        E: en::Encoder<'en>,
        Rows: en::IntoStream<'en> + 'en,
    {
        match self {
            Self::Leaf(keys) => encode_node(encoder, rows(keys), None::<&[Uuid]>),
            Self::Index(keys, children) => {
                if children.len() > max_children || !valid_fanout(keys.len(), children.len()) {
                    return Err(en::Error::custom("invalid B+Tree fanout"));
                }

                encode_node(encoder, rows(keys), Some(children.as_slice()))
            }
        }
    }

    /// Decode a node using a bounded row codec and an independent child limit.
    /// Index nodes must have at least one separator and one more child than row.
    pub async fn decode_bounded<M, D>(
        context: M::Context,
        decoder: &mut D,
        max_children: usize,
    ) -> Result<Self, D::Error>
    where
        M: de::FromStream + Into<Vec<R>>,
        D: de::Decoder,
    {
        match decode_node::<M, _>(context, decoder, max_children).await? {
            Node::Leaf(rows) => Ok(Self::Leaf(rows.into())),
            Node::Index(rows, children) => {
                let rows = rows.into();
                if !valid_fanout(rows.len(), children.len()) {
                    return Err(de::Error::custom("invalid B+Tree fanout"));
                }

                Ok(Self::Index(rows, children))
            }
        }
    }
}

#[cfg(feature = "stream")]
fn valid_fanout(rows: usize, children: usize) -> bool {
    rows > 0 && children.checked_sub(1) == Some(rows)
}

#[cfg(feature = "stream")]
async fn decode_node<N, D>(
    context: N::Context,
    decoder: &mut D,
    max_children: usize,
) -> Result<Node<N>, D::Error>
where
    N: de::FromStream,
    D: de::Decoder,
{
    let mut fields = decoder.open_container(de::Kind::Seq, None).await?;
    if fields.slot() == de::Slot::End {
        return Err(de::Error::custom("missing B+Tree leaf flag"));
    }

    let leaf = bool::from_stream((), decoder).await?;
    decoder.finish_child(&mut fields).await?;
    if fields.slot() == de::Slot::End {
        return Err(de::Error::custom("missing B+Tree rows"));
    }

    let rows = N::from_stream(context, decoder).await?;
    decoder.finish_child(&mut fields).await?;
    let node = if leaf {
        Node::Leaf(rows)
    } else {
        if fields.slot() == de::Slot::End {
            return Err(de::Error::custom("missing B+Tree children"));
        }

        let mut seq = decoder.open_container(de::Kind::Seq, None).await?;
        let mut children = Vec::new();
        while seq.slot() != de::Slot::End {
            if children.len() == max_children {
                return Err(de::Error::custom("too many B+Tree children"));
            }

            let child = Uuid::from_stream((), decoder).await?;
            decoder.finish_child(&mut seq).await?;
            children.try_reserve(1).map_err(de::Error::custom)?;
            children.push(child);
        }

        decoder.finish_child(&mut fields).await?;
        Node::Index(rows, children.into_boxed_slice().into_vec())
    };

    if fields.slot() == de::Slot::End {
        Ok(node)
    } else {
        Err(de::Error::custom("unexpected B+Tree node field"))
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
        decode_node(cxt, decoder, usize::MAX).await
    }
}

#[cfg(feature = "stream")]
fn encode_node<'en, E, R, C>(encoder: E, rows: R, children: Option<C>) -> Result<E::Ok, E::Error>
where
    E: en::Encoder<'en>,
    R: en::IntoStream<'en> + 'en,
    C: en::IntoStream<'en> + 'en,
{
    use en::IntoStream;

    match children {
        Some(children) => (false, rows, children).into_stream(encoder),
        None => (true, rows).into_stream(encoder),
    }
}

#[cfg(feature = "stream")]
impl<'en, N: 'en> en::IntoStream<'en> for Node<N>
where
    N: en::IntoStream<'en>,
{
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Node::Leaf(keys) => encode_node(encoder, keys, None::<Vec<Uuid>>),
            Node::Index(keys, children) => encode_node(encoder, keys, Some(children)),
        }
    }
}

#[cfg(feature = "stream")]
impl<'en, N: 'en> en::ToStream<'en> for Node<N>
where
    N: en::ToStream<'en>,
{
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Node::Leaf(keys) => encode_node(encoder, keys, None::<&[Uuid]>),
            Node::Index(keys, children) => encode_node(encoder, keys, Some(children.as_slice())),
        }
    }
}

#[cfg(debug_assertions)]
impl<N: fmt::Debug> fmt::Debug for Node<N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Leaf(keys) => write!(f, "leaf node with keys {:?}", keys),
            Self::Index(separators, children) => write!(
                f,
                "index node with separators {:?} and children {:?}",
                separators, children
            ),
        }
    }
}

#[cfg(not(debug_assertions))]
impl<V: fmt::Debug> fmt::Debug for Node<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Leaf(_keys) => write!(f, "a leaf node"),
            Self::Index(_separators, children) => {
                write!(f, "an index node with {} children", children.len())
            }
        }
    }
}
