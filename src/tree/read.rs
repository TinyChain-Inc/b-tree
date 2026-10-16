//! Bounded native reads and ordered node traversal.

use std::borrow::Borrow;
use std::pin::Pin;
use std::{fmt, io};

use collate::{Collate, OverlapsValue};
use freqfs::{DirDeref, FileLoad, FileReadGuard};
use futures::future::{self, Future};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use futures::{TryFutureExt, try_join};
use safecast::AsType;
use smallvec::SmallVec;
use uuid::Uuid;

use super::{BTree, Collator, Key, Keys, Leaf, Node, ROOT, Schema};
use crate::NODE_STACK_SIZE;
use crate::group::GroupBy;
use crate::node::Block;
use crate::range::Range;

type NodeStack<V> = SmallVec<[Key<V>; NODE_STACK_SIZE]>;
type Nodes<FE, V> = Pin<Box<dyn Stream<Item = Result<Leaf<FE, V>, io::Error>> + Send>>;
type ResultNodes<FE, V> = Result<Nodes<FE, V>, io::Error>;

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value>,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
    G: DirDeref<Entry = FE>,
    Node<S::Value>: fmt::Debug,
{
    /// Return `true` if this B+Tree contains the given `key`.
    pub async fn contains(&self, key: &[S::Value]) -> Result<bool, io::Error> {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        loop {
            match &*node {
                Node::Leaf(keys) => {
                    let i = keys.bisect_left(key, &self.collator);

                    break Ok(if i < keys.len() {
                        match keys.get(i) {
                            Some(present) => present == key,
                            _ => false,
                        }
                    } else {
                        false
                    });
                }
                Node::Index(bounds, children) => {
                    let i = bounds.bisect_right(key, &self.collator);

                    if i == 0 {
                        return Ok(false);
                    } else {
                        node = self.dir.as_dir().read_file(&children[i - 1]).await?;
                    }
                }
            }
        }
    }

    /// Count how many keys lie within the given `range` of this B+Tree.
    pub async fn count<BV>(&self, range: &Range<BV>) -> Result<u64, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        let root = self.dir.as_dir().read_file(&ROOT).await?;
        self.count_inner(range, root).await
    }

    fn count_inner<'a, BV>(
        &'a self,
        range: &'a Range<BV>,
        node: FileReadGuard<'a, Node<S::Value>>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, io::Error>> + 'a>>
    where
        BV: Borrow<S::Value> + 'a,
    {
        Box::pin(async move {
            match &*node {
                Node::Leaf(keys) if range.is_empty() => Ok(keys.len() as u64),
                Node::Leaf(keys) => {
                    let (l, r) = keys.bisect(range, &self.collator);

                    if l == keys.len() {
                        Ok(0)
                    } else if l == r {
                        if range.contains_value(&keys[l], &self.collator) {
                            Ok(1)
                        } else {
                            Ok(0)
                        }
                    } else {
                        Ok((r - l) as u64)
                    }
                }
                Node::Index(_bounds, children) if range.is_empty() => {
                    stream::iter(children)
                        .then(|node_id| self.dir.as_dir().read_file(node_id))
                        .map_ok(|node| self.count_inner(range, node))
                        .try_buffer_unordered(num_cpus::get())
                        .try_fold(0, |sum, count| future::ready(Ok(sum + count)))
                        .await
                }
                Node::Index(bounds, children) => {
                    let (l, r) = bounds.bisect(range, &self.collator);
                    let l = if l == 0 { l } else { l - 1 };

                    if l == children.len() {
                        let node = self
                            .dir
                            .as_dir()
                            .read_file(children.last().expect("last"))
                            .await?;

                        self.count_inner(range, node).await
                    } else if l == r || l + 1 == r {
                        let node = self.dir.as_dir().read_file(&children[l]).await?;
                        self.count_inner(range, node).await
                    } else {
                        let left = self
                            .dir
                            .as_dir()
                            .read_file(&children[l])
                            .and_then(|node| self.count_inner(range, node));

                        let default_range = Range::<S::Value>::default();

                        let middle = stream::iter(&children[(l + 1)..(r - 1)])
                            .then(|node_id| self.dir.as_dir().read_file(node_id))
                            .map_ok(|node| self.count_inner(&default_range, node))
                            .try_buffer_unordered(num_cpus::get())
                            .try_fold(0, |sum, count| future::ready(Ok(sum + count)));

                        let right = self
                            .dir
                            .as_dir()
                            .read_file(&children[r - 1])
                            .and_then(|node| self.count_inner(range, node));

                        let (left, middle, right) = try_join!(left, middle, right)?;

                        Ok(left + middle + right)
                    }
                }
            }
        })
    }

    /// Return the first key in this B+Tree within the given `range`, if any.
    pub async fn first<BV>(&self, range: Range<BV>) -> Result<Option<Key<S::Value>>, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        match &*node {
            Node::Leaf(keys) if keys.is_empty() => return Ok(None),
            _ => {}
        }

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (l, _r) = keys.bisect(&range, &self.collator);

                    break if l == keys.len() {
                        None
                    } else if range.contains_value(&keys[l], &self.collator) {
                        Some(stack_key(&keys[l]))
                    } else {
                        None
                    };
                }
                Node::Index(bounds, children) => {
                    let (l, _r) = bounds.bisect(&range, &self.collator);

                    if l == bounds.len() {
                        node = self
                            .dir
                            .as_dir()
                            .read_file(children.last().expect("last"))
                            .await?;
                    } else if range.contains_value(&bounds[l], &self.collator) {
                        break Some(stack_key(&bounds[l]));
                    } else {
                        node = self
                            .dir
                            .as_dir()
                            .read_file(&children[l.saturating_sub(1)])
                            .await?;
                    }
                }
            }
        })
    }

    /// Return the last key in this B+Tree with the given `prefix`, if any.
    pub async fn last<BV>(&self, range: Range<BV>) -> Result<Option<Key<S::Value>>, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        match &*node {
            Node::Leaf(keys) if keys.is_empty() => return Ok(None),
            _ => {}
        }

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (_l, r) = keys.bisect(&range, &self.collator);

                    break if r == keys.len() {
                        if range.contains_value(&keys[r - 1], &self.collator) {
                            Some(stack_key(&keys[r - 1]))
                        } else {
                            None
                        }
                    } else if range.contains_value(&keys[r], &self.collator) {
                        Some(stack_key(&keys[r]))
                    } else {
                        None
                    };
                }
                Node::Index(bounds, children) => {
                    let (_l, r) = bounds.bisect(&range, &self.collator);

                    if r == 0 {
                        break None;
                    } else {
                        node = self.dir.as_dir().read_file(&children[r - 1]).await?;
                    }
                }
            }
        })
    }

    /// Return `true` if the given `range` of this B+Tree contains zero keys.
    pub async fn is_empty<R: Borrow<Range<S::Value>>>(&self, range: R) -> Result<bool, io::Error> {
        let range = range.borrow();

        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (l, r) = keys.bisect(range, &self.collator);
                    break l == r;
                }
                Node::Index(bounds, children) => {
                    let (l, r) = bounds.bisect(range, &self.collator);

                    if l == children.len() {
                        node = self.dir.as_dir().read_file(&children[l - 1]).await?;
                    } else if l == r {
                        node = self.dir.as_dir().read_file(&children[l]).await?;
                    } else {
                        break false;
                    }
                }
            }
        })
    }

    #[cfg(debug_assertions)]
    fn is_valid_node<'a>(
        &'a self,
        node: &'a Node<S::Value>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, io::Error>> + 'a>> {
        Box::pin(async move {
            let order = self.schema.order();

            match node {
                Node::Leaf(keys) => {
                    assert!(keys.len() >= (order / 2) - 1);
                    assert!(keys.len() < order);
                }
                Node::Index(bounds, children) => {
                    assert_eq!(bounds.len(), children.len());
                    assert!(children.len() >= self.schema.order() / 2);
                    assert!(children.len() <= order);

                    for (left, node_id) in bounds.iter().zip(children) {
                        let node = self.dir.as_dir().read_file(node_id).await?;

                        match &*node {
                            Node::Leaf(keys) => assert_eq!(left, &keys[0]),
                            Node::Index(bounds, _) => assert_eq!(left, &bounds[0]),
                        }

                        assert!(self.is_valid_node(&*node).await?);
                    }
                }
            }

            Ok(true)
        })
    }
}

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<S::Value>: fmt::Debug,
{
    /// Construct a [`Stream`] of all the keys in the given `range` of this B+Tree.
    pub async fn keys<BV>(self, range: Range<BV>) -> Result<Keys<S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'static,
    {
        let nodes = nodes_forward(self.dir, self.collator, range, ROOT).await?;

        let keys = nodes
            .map_ok(move |leaf| {
                let mut keys = NodeStack::with_capacity(leaf.as_ref().len());

                for key in leaf.as_ref() {
                    keys.push(key.iter().cloned().collect())
                }

                stream::iter(keys).map(Ok)
            })
            .try_flatten();

        Ok(Box::pin(keys))
    }

    pub async fn keys_rev<BV>(self, range: Range<BV>) -> Result<Keys<S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'static,
    {
        let nodes = nodes_reverse(self.dir, self.collator, range, ROOT).await?;

        let keys = nodes
            .map_ok(|leaf| {
                let mut keys = NodeStack::with_capacity(leaf.as_ref().len());

                for key in leaf.as_ref().iter().rev() {
                    keys.push(key.iter().cloned().collect());
                }

                stream::iter(keys).map(Ok)
            })
            .try_flatten();

        Ok(Box::pin(keys))
    }

    /// Construct a [`Stream`] of unique length-`n` prefixes within the given `range`.
    pub async fn groups<BV>(
        self,
        range: Range<BV>,
        n: usize,
        reverse: bool,
    ) -> Result<Keys<S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'static,
    {
        if n <= self.schema.len() {
            let collator = self.collator.clone();

            let nodes = if reverse {
                nodes_reverse(self.dir, self.collator, range, ROOT).await?
            } else {
                nodes_forward(self.dir, self.collator, range, ROOT).await?
            };

            let groups = GroupBy::new(collator, nodes, n, reverse);
            Ok(Box::pin(groups))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "a table with {} columns does not have prefix groups of length {n}",
                    self.schema.len()
                ),
            ))
        }
    }

    #[cfg(debug_assertions)]
    pub async fn is_valid(self) -> Result<bool, io::Error> {
        {
            let root = self.dir.as_dir().read_file(&ROOT).await?;

            match &*root {
                Node::Leaf(keys) => {
                    assert!(keys.len() <= self.schema.order());
                }
                Node::Index(bounds, children) => {
                    assert_eq!(bounds.len(), children.len());

                    for (left, node_id) in bounds.iter().zip(children) {
                        let node = self.dir.as_dir().read_file(node_id).await?;

                        match &*node {
                            Node::Leaf(keys) => assert_eq!(left, &keys[0]),
                            Node::Index(bounds, _) => assert_eq!(left, &bounds[0]),
                        }

                        assert!(self.is_valid_node(&*node).await?);
                    }
                }
            }
        }

        let default_range = Range::<S::Value>::default();
        let count = self.count(&default_range).await? as usize;
        let mut contents = Vec::with_capacity(count);
        let mut stream = self.keys(default_range).await?;
        while let Some(key) = stream.try_next().await? {
            contents.push(key);
        }

        assert_eq!(count, contents.len());

        Ok(true)
    }
}

enum NodeRead {
    Excluded,
    Child(Uuid),
    Children(Box<SmallVec<[Uuid; NODE_STACK_SIZE]>>),
    Leaf((usize, usize)),
}

impl fmt::Debug for NodeRead {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Excluded => f.write_str("(none)"),
            Self::Child(id) => write!(f, "child {id}"),
            Self::Children(ids) => write!(f, "children {:?}", *ids),
            Self::Leaf((start, end)) => write!(f, "leaf records [{start}..{end})"),
        }
    }
}

fn nodes_forward<C, V, BV, FE, G>(
    dir: G,
    collator: Collator<C>,
    range: Range<BV>,
    node_id: Uuid,
) -> Pin<Box<dyn Future<Output = ResultNodes<FE, V>> + Send>>
where
    C: Collate<Value = V> + Clone + Send + Sync + 'static,
    V: Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
    BV: Borrow<V> + Clone + Send + Sync + 'static,
    FE: AsType<Node<V>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
{
    #[cfg(feature = "logging")]
    log::debug!("reading BTree keys in forward order");

    let Some(file) = dir.as_dir().get_file(&node_id).cloned() else {
        return Box::pin(future::ready(Err(io::Error::new(
            io::ErrorKind::NotFound,
            node_id.to_string(),
        ))));
    };

    let fut = file.into_read().map_ok(move |node| {
        let read = match &*node {
            Node::Leaf(keys) if range.is_empty() => NodeRead::Leaf((0, keys.len())),
            Node::Leaf(keys) => {
                let (l, r) = keys.bisect(&range, &collator);

                if r == 0 || l == keys.len() {
                    NodeRead::Excluded
                } else if l == r {
                    if range.contains_value(&keys[l], &collator) {
                        NodeRead::Leaf((l, l + 1))
                    } else {
                        NodeRead::Excluded
                    }
                } else {
                    NodeRead::Leaf((l, r))
                }
            }
            Node::Index(_bounds, children) if range.is_empty() => {
                debug_assert!(!children.is_empty());

                if children.len() == 1 {
                    NodeRead::Child(children[0])
                } else {
                    NodeRead::Children(Box::new(SmallVec::from_slice(children)))
                }
            }
            Node::Index(bounds, children) => {
                let (l, r) = bounds.bisect(&range, &collator);
                let l = if l == 0 { l } else { l - 1 };

                if r == 0 {
                    NodeRead::Excluded
                } else if l == children.len() {
                    NodeRead::Child(children[l - 1])
                } else if l == r || l + 1 == r {
                    NodeRead::Child(children[l])
                } else {
                    NodeRead::Children(Box::new(SmallVec::from_slice(&children[l..r])))
                }
            }
        };

        #[cfg(feature = "logging")]
        log::trace!("read {read:?}");

        let nodes: Nodes<FE, V> = match read {
            NodeRead::Excluded => {
                let nodes = stream::empty();
                Box::pin(nodes)
            }
            NodeRead::Child(node_id) => {
                let nodes =
                    stream::once(nodes_forward(dir, collator, range, node_id)).try_flatten();

                Box::pin(nodes)
            }
            NodeRead::Children(children) => {
                let last_child = children.len() - 1;
                let nodes = stream::iter(children.into_iter().enumerate())
                    .map(move |(i, node_id)| {
                        if i == 0 || i == last_child {
                            nodes_forward(dir.clone(), collator.clone(), range.clone(), node_id)
                        } else {
                            nodes_forward(
                                dir.clone(),
                                collator.clone(),
                                Range::<V>::default(),
                                node_id,
                            )
                        }
                    })
                    .buffered(2)
                    .try_flatten();

                Box::pin(nodes)
            }
            NodeRead::Leaf(range) => {
                let nodes = stream::once(future::ready(Ok(Leaf::new(node, range))));
                Box::pin(nodes)
            }
        };

        nodes
    });

    Box::pin(fut)
}

fn nodes_reverse<C, V, BV, FE, G>(
    dir: G,
    collator: Collator<C>,
    range: Range<BV>,
    node_id: Uuid,
) -> Pin<Box<dyn Future<Output = ResultNodes<FE, V>> + Send>>
where
    C: Collate<Value = V> + Clone + Send + Sync + 'static,
    V: Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
    BV: Borrow<V> + Clone + Send + Sync + 'static,
    FE: AsType<Node<V>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
{
    let Some(file) = dir.as_dir().get_file(&node_id).cloned() else {
        return Box::pin(future::ready(Err(io::Error::new(
            io::ErrorKind::NotFound,
            node_id.to_string(),
        ))));
    };

    let fut = file.into_read().map_ok(move |node| {
        let read = match &*node {
            Node::Leaf(keys) if range.is_empty() => NodeRead::Leaf((0, keys.len())),
            Node::Leaf(keys) => {
                let (l, r) = keys.bisect(&range, &collator);

                if l == keys.len() || r == 0 {
                    NodeRead::Excluded
                } else if l == r {
                    if range.contains_value(&keys[l], &collator) {
                        NodeRead::Leaf((l, l + 1))
                    } else {
                        NodeRead::Excluded
                    }
                } else {
                    NodeRead::Leaf((l, r))
                }
            }
            Node::Index(_bounds, children) if range.is_empty() => {
                debug_assert!(!children.is_empty());

                if children.len() == 1 {
                    NodeRead::Child(children[0])
                } else {
                    NodeRead::Children(Box::new(SmallVec::from_slice(children)))
                }
            }
            Node::Index(bounds, children) => {
                debug_assert!(!children.is_empty());

                let (l, r) = bounds.bisect(&range, &collator);
                let l = if l == 0 { l } else { l - 1 };

                if r == 0 {
                    NodeRead::Excluded
                } else if l == children.len() {
                    NodeRead::Child(children[l - 1])
                } else if l == r || l + 1 == r {
                    NodeRead::Child(children[l])
                } else {
                    NodeRead::Children(Box::new(SmallVec::from_slice(&children[l..r])))
                }
            }
        };

        let nodes: Nodes<FE, V> = match read {
            NodeRead::Excluded => {
                let nodes = stream::empty();
                Box::pin(nodes)
            }
            NodeRead::Child(node_id) => {
                let nodes =
                    stream::once(nodes_reverse(dir, collator, range, node_id)).try_flatten();

                Box::pin(nodes)
            }
            NodeRead::Children(children) => {
                let last_child = children.len() - 1;
                let nodes = stream::iter(children.into_iter().enumerate().rev())
                    .map(move |(i, node_id)| {
                        if i == 0 || i == last_child {
                            nodes_reverse(dir.clone(), collator.clone(), range.clone(), node_id)
                        } else {
                            nodes_reverse(
                                dir.clone(),
                                collator.clone(),
                                Range::<V>::default(),
                                node_id,
                            )
                        }
                    })
                    .buffered(2)
                    .try_flatten();

                Box::pin(nodes)
            }
            NodeRead::Leaf(range) => {
                let nodes = stream::once(future::ready(Ok(Leaf::new(node, range))));
                Box::pin(nodes)
            }
        };

        nodes
    });

    Box::pin(fut)
}

#[inline]
fn stack_key<'a, T, A>(iter: A) -> SmallVec<[T; NODE_STACK_SIZE]>
where
    T: Clone + 'a,
    A: IntoIterator<Item = &'a T>,
{
    iter.into_iter().cloned().collect()
}
