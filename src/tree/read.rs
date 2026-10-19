//! Bounded native reads and ordered node traversal.

use std::borrow::Borrow;
use std::pin::Pin;
use std::{fmt, io};

use collate::{Collate, OverlapsValue};
use freqfs::{DirDeref, FileLoad, FileReadGuard};
use futures::future::{self, Future, FutureExt};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use safecast::AsType;
use smallvec::SmallVec;
use uuid::Uuid;

use super::{BTree, Collator, Key, Keys, Leaf, Node, ROOT, Schema, validate_children};
use crate::NODE_STACK_SIZE;
use crate::group::GroupBy;
use crate::node::Block;
use crate::range::Range;

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
    /// Return whether the complete semantic key is present, independently of collation ties.
    pub async fn contains(&self, key: &[S::Value]) -> Result<bool, io::Error> {
        if key.len() != self.schema.len() {
            return Ok(false);
        }

        Ok(self.locate(key).await?.1)
    }

    /// Return the unique row with this semantic prefix, or an ambiguity error.
    /// Collation-equivalent but unequal prefixes do not match.
    pub async fn get(&self, prefix: &[S::Value]) -> Result<Option<Key<S::Value>>, io::Error> {
        let mut found = None;
        self.visit_equivalent(prefix, |_, row| {
            if row.starts_with(prefix) {
                if found.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "ambiguous semantic key",
                    ));
                }

                found = Some(stack_key(row));
            }

            Ok(false)
        })
        .await?;

        Ok(found)
    }

    pub(super) async fn locate<V: Borrow<S::Value>>(
        &self,
        key: &[V],
    ) -> Result<(Vec<(Uuid, usize)>, bool), io::Error> {
        let mut found = false;
        let path = self
            .visit_equivalent(key, |_, row| {
                found = row.len() >= key.len() && row.iter().zip(key).all(|(a, b)| a == b.borrow());

                Ok(found)
            })
            .await?;

        Ok((path, found))
    }

    // Keep only one root-to-leaf path and one borrowed node. The returned slot
    // is also an insertion position when no matching value is found.
    pub(super) async fn visit_equivalent<V: Borrow<S::Value>>(
        &self,
        key: &[V],
        mut visit: impl FnMut(&[(Uuid, usize)], &[S::Value]) -> Result<bool, io::Error>,
    ) -> Result<Vec<(Uuid, usize)>, io::Error> {
        let mut path = Vec::new();
        let mut id = ROOT;
        let mut node = loop {
            let node = self.dir.as_dir().read_file(&id).await?;

            match &*node {
                Node::Index(separators, children) => {
                    validate_children(separators, children)?;
                    let slot = separators.partition_point(|separator| {
                        self.collator.cmp_routing(separator, key).is_lt()
                    });
                    path.push((id, slot));
                    id = children[slot];
                }
                Node::Leaf(rows) => {
                    path.push((id, rows.bisect_left(key, &self.collator)));
                    break node;
                }
            }
        };

        loop {
            let start = path.last().expect("leaf path").1;
            let Node::Leaf(rows) = &*node else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "expected leaf"));
            };

            for (slot, row) in rows.iter().enumerate().skip(start) {
                path.last_mut().expect("leaf path").1 = slot;
                if !self.collator.cmp_slices(row, key).is_eq() || visit(&path, row)? {
                    return Ok(path);
                }
            }

            path.last_mut().expect("leaf path").1 = rows.len();
            drop(node);

            // Do not move an absent key into a subtree beyond its routing
            // interval. Equal separators remain candidates for semantic identity.
            match self
                .adjacent_leaf_until(&mut path, true, |separator| {
                    self.collator.cmp_slices(separator, key).is_gt()
                })
                .await?
            {
                Some(next) => node = next,
                None => return Ok(path),
            }
        }
    }

    // Retain the full leaf path when there is no neighbor, including its insertion
    // position. Return the acquired leaf so callers need not release and reread it.
    pub(super) async fn adjacent_leaf<'a>(
        &'a self,
        path: &mut Vec<(Uuid, usize)>,
        forward: bool,
    ) -> io::Result<Option<FileReadGuard<'a, Node<S::Value>>>> {
        self.adjacent_leaf_until(path, forward, |_| false).await
    }

    async fn adjacent_leaf_until<'a>(
        &'a self,
        path: &mut Vec<(Uuid, usize)>,
        forward: bool,
        stop: impl Fn(&[S::Value]) -> bool,
    ) -> io::Result<Option<FileReadGuard<'a, Node<S::Value>>>> {
        for level in (0..path.len().saturating_sub(1)).rev() {
            let (id, slot) = path[level];
            let node = self.dir.as_dir().read_file(&id).await?;
            let Node::Index(rows, children) = &*node else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected index ancestor",
                ));
            };

            validate_children(rows, children)?;
            if slot >= children.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid child slot",
                ));
            }

            let next = if forward {
                slot.checked_add(1).filter(|i| *i < children.len())
            } else {
                slot.checked_sub(1)
            };
            let Some(next) = next else { continue };
            let separator = &rows[if forward { slot } else { next }];
            if stop(separator) {
                return Ok(None);
            }

            path.truncate(level);
            path.push((id, next));
            let mut child = children[next];
            drop(node);

            loop {
                let node = self.dir.as_dir().read_file(&child).await?;

                match &*node {
                    Node::Leaf(rows) => {
                        if rows.is_empty() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "empty non-root leaf",
                            ));
                        }

                        path.push((child, if forward { 0 } else { rows.len() - 1 }));
                        return Ok(Some(node));
                    }
                    Node::Index(rows, children) => {
                        validate_children(rows, children)?;
                        let slot = if forward { 0 } else { children.len() - 1 };
                        path.push((child, slot));
                        child = children[slot];
                    }
                }
            }
        }

        Ok(None)
    }

    /// Count how many keys lie within the given `range` of this B+Tree.
    pub async fn count<BV>(&self, range: &Range<BV>) -> Result<u64, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        validate_range(range, self.schema.len())?;
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
                Node::Index(separators, children) => {
                    validate_children(separators, children)?;
                    let (start, end) = range.child_slots(separators, &self.collator);
                    if start >= end {
                        return Ok(0);
                    }

                    stream::iter(&children[start..end])
                        .then(|node_id| self.dir.as_dir().read_file(node_id))
                        .map_ok(|node| self.count_inner(range, node))
                        .try_buffer_unordered(num_cpus::get().max(1))
                        .try_fold(0, |sum, count| future::ready(Ok(sum + count)))
                        .await
                }
            }
        })
    }

    /// Return the first key in this B+Tree within the given `range`, if any.
    pub async fn first<BV>(&self, range: Range<BV>) -> Result<Option<Key<S::Value>>, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        validate_range(&range, self.schema.len())?;
        self.edge(&range, false).await
    }

    /// Return the last key in this B+Tree within the given `range`, if any.
    pub async fn last<BV>(&self, range: Range<BV>) -> Result<Option<Key<S::Value>>, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        validate_range(&range, self.schema.len())?;
        self.edge(&range, true).await
    }

    async fn edge<BV>(&self, range: &Range<BV>, reverse: bool) -> io::Result<Option<Key<S::Value>>>
    where
        BV: Borrow<S::Value>,
    {
        let mut path = Vec::new();
        let mut id = ROOT;
        let mut node = loop {
            let node = self.dir.as_dir().read_file(&id).await?;
            match &*node {
                Node::Index(separators, children) => {
                    validate_children(separators, children)?;
                    let (start, end) = range.child_slots(separators, &self.collator);
                    if start >= end {
                        return Ok(None);
                    }

                    let slot = if reverse { end - 1 } else { start };
                    path.push((id, slot));
                    id = children[slot];
                }
                Node::Leaf(_) => {
                    path.push((id, 0));
                    break node;
                }
            }
        };

        loop {
            let Node::Leaf(rows) = &*node else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "expected leaf"));
            };
            let (start, end) = rows.bisect(range, &self.collator);
            if start < end {
                return Ok(Some(stack_key(
                    &rows[if reverse { end - 1 } else { start }],
                )));
            }
            drop(node);

            match self
                .adjacent_leaf_until(&mut path, !reverse, |separator| {
                    let order = range.cmp_endpoint(separator, reverse, &self.collator);
                    if reverse {
                        order.is_lt()
                    } else {
                        order.is_gt()
                    }
                })
                .await?
            {
                Some(next) => node = next,
                None => return Ok(None),
            }
        }
    }

    /// Return `true` if the given `range` of this B+Tree contains zero keys.
    pub async fn is_empty<R: Borrow<Range<S::Value>>>(&self, range: R) -> Result<bool, io::Error> {
        self.first(range.borrow().as_ref())
            .await
            .map(|first| first.is_none())
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
        validate_range(&range, self.schema.len())?;
        let nodes = nodes(self.dir, self.collator, range, ROOT, false).await?;

        let keys = nodes
            .map_ok(move |leaf| {
                // Rows already contain inline key storage; keep the outer batch on the
                // heap to avoid oversized temporaries when polling stream combinators.
                let mut keys: Vec<Key<S::Value>> = Vec::with_capacity(leaf.as_ref().len());

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
        validate_range(&range, self.schema.len())?;
        let nodes = nodes(self.dir, self.collator, range, ROOT, true).await?;

        let keys = nodes
            .map_ok(|leaf| {
                // Rows already contain inline key storage; keep the outer batch on the
                // heap to avoid oversized temporaries when polling stream combinators.
                let mut keys: Vec<Key<S::Value>> = Vec::with_capacity(leaf.as_ref().len());

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
            validate_range(&range, self.schema.len())?;
            let collator = self.collator.clone();

            let nodes = nodes(self.dir, self.collator, range, ROOT, reverse).await?;

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
}

enum NodeRead {
    Excluded,
    Child(Uuid),
    // Keep the enum small without boxing inline storage. A boxed SmallVec would
    // allocate even for short lists and allocate again when it spills.
    Children(Vec<Uuid>),
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

fn nodes<C, V, BV, FE, G>(
    dir: G,
    collator: Collator<C>,
    range: Range<BV>,
    node_id: Uuid,
    reverse: bool,
) -> Pin<Box<dyn Future<Output = ResultNodes<FE, V>> + Send>>
where
    C: Collate<Value = V> + Clone + Send + Sync + 'static,
    V: Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
    BV: Borrow<V> + Clone + Send + Sync + 'static,
    FE: AsType<Node<V>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
{
    #[cfg(feature = "logging")]
    log::debug!("reading BTree keys (reverse: {reverse})");

    let Some(file) = dir.as_dir().get_file(&node_id).cloned() else {
        return Box::pin(future::ready(Err(io::Error::new(
            io::ErrorKind::NotFound,
            node_id.to_string(),
        ))));
    };

    let fut = file.into_read().map(move |node| {
        let node = node?;
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
            Node::Index(separators, children) => {
                validate_children(separators, children)?;
                let (start, end) = range.child_slots(separators, &collator);
                if start >= end {
                    NodeRead::Excluded
                } else if end - start == 1 {
                    NodeRead::Child(children[start])
                } else {
                    NodeRead::Children(children[start..end].to_vec())
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
                    stream::once(nodes(dir, collator, range, node_id, reverse)).try_flatten();

                Box::pin(nodes)
            }
            NodeRead::Children(children) => {
                let len = children.len();
                let nodes = stream::iter(0..len)
                    .map(move |i| {
                        let slot = if reverse { len - i - 1 } else { i };
                        nodes(
                            dir.clone(),
                            collator.clone(),
                            range.clone(),
                            children[slot],
                            reverse,
                        )
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

        Ok(nodes)
    });

    Box::pin(fut)
}

// Full-width prefixes remain valid. Bounds select one additional column, which
// must exist so routing endpoints and complete-row predicates describe the same range.
fn validate_range<V>(range: &Range<V>, columns: usize) -> io::Result<()> {
    if range.len() > columns {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "BTree range references a column outside its schema",
        ))
    } else {
        Ok(())
    }
}

#[inline]
fn stack_key<'a, T, A>(iter: A) -> SmallVec<[T; NODE_STACK_SIZE]>
where
    T: Clone + 'a,
    A: IntoIterator<Item = &'a T>,
{
    iter.into_iter().cloned().collect()
}
