//! Bounded native reads and ordered node traversal.

use std::{
    borrow::Borrow,
    fmt, io,
    ops::{ControlFlow, Deref},
    pin::Pin,
};

use collate::{Collate, OverlapsValue};
use freqfs::{DirDeref, FileLoad, FileReadGuard};
use futures::future::{self, Future, FutureExt};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use safecast::AsType;
use smallvec::SmallVec;
use uuid::Uuid;

use super::{BTree, Collator, Key, Leaf, Node, ROOT, Schema, validate_index, validate_rows};
use crate::NODE_STACK_SIZE;
use crate::node::Block;
use crate::range::Range;

type ScopedKeys<'a, V> = Pin<Box<dyn Stream<Item = Result<Key<V>, io::Error>> + Send + 'a>>;

type Nodes<'a, FE, V> = Pin<Box<dyn Stream<Item = Result<Leaf<FE, V>, io::Error>> + Send + 'a>>;
type ResultNodes<'a, FE, V> = Result<Nodes<'a, FE, V>, io::Error>;

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
        self.unique(prefix, |_, row| stack_key(row)).await
    }

    /// Read a unique semantic-prefix match without cloning its values.
    ///
    /// The callback runs synchronously after the complete equivalent group has
    /// been checked for ambiguity. It may borrow caller-owned data, but cannot
    /// retain the row or reacquire storage protected by this tree owner. A found
    /// row requires one additional leaf read after the lookup releases its guards.
    /// Missing or ambiguous prefixes never invoke the callback.
    ///
    /// Borrowed rows cannot escape through the callback's result:
    ///
    /// ```compile_fail
    /// use std::io;
    /// use b_tree::{BTree, Node, Schema, collate::Collate};
    /// use freqfs::{DirDeref, FileLoad};
    /// use safecast::AsType;
    ///
    /// async fn escape<S, C, FE, G>(tree: &BTree<S, C, G>, key: &[S::Value])
    /// where
    ///     S: Schema,
    ///     C: Collate<Value = S::Value>,
    ///     FE: AsType<Node<Vec<Vec<S::Value>>>> + Send + Sync + FileLoad,
    ///     G: DirDeref<Entry = FE>,
    /// {
    ///     let _row = tree.get_with(key, |row| Ok::<_, io::Error>(row)).await;
    /// }
    /// ```
    pub async fn get_with<R, E>(
        &self,
        prefix: &[S::Value],
        read: impl FnOnce(&[S::Value]) -> Result<R, E> + Send,
    ) -> Result<Option<R>, E>
    where
        E: From<io::Error>,
    {
        let Some((id, slot)) = self
            .unique(prefix, |path, _| *path.last().expect("leaf path"))
            .await?
        else {
            return Ok(None);
        };

        let node = self
            .dir
            .as_dir()
            .read_file(id.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
            .await
            .map_err(super::required_node_error)?;
        let Node::Leaf(rows) = &*node else {
            return Err(freqfs::IntegrityError::invalid_data("expected leaf").into());
        };
        validate_rows(rows, self.schema.len())?;
        let row = rows
            .get(slot)
            .filter(|row| row.starts_with(prefix))
            .ok_or_else(|| {
                freqfs::IntegrityError::invalid_data(
                    "resolved row does not match its semantic prefix",
                )
            })?;

        read(row).map(Some)
    }

    async fn unique<R>(
        &self,
        prefix: &[S::Value],
        mut select: impl FnMut(&[(Uuid, usize)], &[S::Value]) -> R,
    ) -> Result<Option<R>, io::Error> {
        let mut found = None;
        self.visit_equivalent(prefix, |path, row| {
            if row.starts_with(prefix) {
                if found.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "ambiguous semantic key",
                    ));
                }

                found = Some(select(path, row));
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
            if path.iter().any(|(ancestor, _)| *ancestor == id) {
                return Err(freqfs::IntegrityError::invalid_data(
                    "cycle in BTree ancestry",
                ));
            }

            let node = self
                .dir
                .as_dir()
                .read_file(id.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
                .await
                .map_err(super::required_node_error)?;

            match &*node {
                Node::Index(separators, children) => {
                    validate_index(&*self.schema, &self.collator, separators, children)?;
                    let slot = separators.partition_point(|separator| {
                        self.collator.cmp_routing(separator, key).is_lt()
                    });
                    path.push((id, slot));
                    id = children[slot];
                }
                Node::Leaf(rows) => {
                    validate_rows(rows, self.schema.len())?;
                    path.push((id, rows.bisect_left(key, &self.collator)));
                    break node;
                }
            }
        };

        loop {
            let start = path.last().expect("leaf path").1;
            let Node::Leaf(rows) = &*node else {
                return Err(freqfs::IntegrityError::invalid_data("expected leaf"));
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
            let node = self
                .dir
                .as_dir()
                .read_file(id.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
                .await
                .map_err(super::required_node_error)?;
            let Node::Index(rows, children) = &*node else {
                return Err(freqfs::IntegrityError::invalid_data(
                    "expected index ancestor",
                ));
            };

            validate_index(&*self.schema, &self.collator, rows, children)?;
            if slot >= children.len() {
                return Err(freqfs::IntegrityError::invalid_data("invalid child slot"));
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
                if path.iter().any(|(ancestor, _)| *ancestor == child) {
                    return Err(freqfs::IntegrityError::invalid_data(
                        "cycle in BTree ancestry",
                    ));
                }

                let node = self
                    .dir
                    .as_dir()
                    .read_file(child.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
                    .await
                    .map_err(super::required_node_error)?;

                match &*node {
                    Node::Leaf(rows) => {
                        validate_rows(rows, self.schema.len())?;
                        if rows.is_empty() {
                            return Err(freqfs::IntegrityError::invalid_data(
                                "empty non-root leaf",
                            ));
                        }

                        path.push((child, if forward { 0 } else { rows.len() - 1 }));
                        return Ok(Some(node));
                    }
                    Node::Index(rows, children) => {
                        validate_index(&*self.schema, &self.collator, rows, children)?;
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
        range.validate(self.schema.len(), &self.collator)?;
        let root = self
            .dir
            .as_dir()
            .read_file(ROOT.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
            .await
            .map_err(super::required_node_error)?;
        self.count_inner(range, root, vec![ROOT]).await
    }

    fn count_inner<'a, BV>(
        &'a self,
        range: &'a Range<BV>,
        node: FileReadGuard<'a, Node<S::Value>>,
        ancestors: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, io::Error>> + 'a>>
    where
        BV: Borrow<S::Value> + 'a,
    {
        Box::pin(async move {
            match &*node {
                Node::Leaf(keys) => {
                    validate_rows(keys, self.schema.len())?;
                    if range.is_empty() {
                        return Ok(keys.len() as u64);
                    }

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
                    validate_index(&*self.schema, &self.collator, separators, children)?;
                    let (start, end) = range.child_slots(separators, &self.collator);
                    if start >= end {
                        return Ok(0);
                    }

                    let children = children[start..end].to_vec();
                    drop(node);
                    stream::iter(children)
                        .then(|node_id| {
                            let mut ancestors = ancestors.clone();
                            async move {
                                if ancestors.contains(&node_id) {
                                    return Err(freqfs::IntegrityError::invalid_data(
                                        "cycle in BTree ancestry",
                                    ));
                                }
                                ancestors.push(node_id);
                                let node = self
                                    .dir
                                    .as_dir()
                                    .read_file(
                                        node_id
                                            .hyphenated()
                                            .encode_lower(&mut Uuid::encode_buffer()),
                                    )
                                    .await
                                    .map_err(super::required_node_error)?;
                                Ok(self.count_inner(range, node, ancestors))
                            }
                        })
                        .try_buffer_unordered(num_cpus::get().max(1))
                        .try_fold(0u64, |sum, count| {
                            future::ready(sum.checked_add(count).ok_or_else(|| {
                                freqfs::IntegrityError::invalid_data("BTree count overflow")
                            }))
                        })
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
        range.validate(self.schema.len(), &self.collator)?;
        self.edge(&range, false, |row| stack_key(row)).await
    }

    /// Return the last key in this B+Tree within the given `range`, if any.
    pub async fn last<BV>(&self, range: Range<BV>) -> Result<Option<Key<S::Value>>, io::Error>
    where
        BV: Borrow<S::Value>,
    {
        range.validate(self.schema.len(), &self.collator)?;
        self.edge(&range, true, |row| stack_key(row)).await
    }

    async fn edge<BV, R>(
        &self,
        range: &Range<BV>,
        reverse: bool,
        read: impl FnOnce(&[S::Value]) -> R,
    ) -> io::Result<Option<R>>
    where
        BV: Borrow<S::Value>,
    {
        let mut path = Vec::new();
        let mut id = ROOT;
        let mut node = loop {
            if path.iter().any(|(ancestor, _)| *ancestor == id) {
                return Err(freqfs::IntegrityError::invalid_data(
                    "cycle in BTree ancestry",
                ));
            }

            let node = self
                .dir
                .as_dir()
                .read_file(id.hyphenated().encode_lower(&mut Uuid::encode_buffer()))
                .await
                .map_err(super::required_node_error)?;
            match &*node {
                Node::Index(separators, children) => {
                    validate_index(&*self.schema, &self.collator, separators, children)?;
                    let (start, end) = range.child_slots(separators, &self.collator);
                    if start >= end {
                        return Ok(None);
                    }

                    let slot = if reverse { end - 1 } else { start };
                    path.push((id, slot));
                    id = children[slot];
                }
                Node::Leaf(rows) => {
                    validate_rows(rows, self.schema.len())?;
                    path.push((id, 0));
                    break node;
                }
            }
        };

        loop {
            let Node::Leaf(rows) = &*node else {
                return Err(freqfs::IntegrityError::invalid_data("expected leaf"));
            };
            let (start, end) = rows.bisect(range, &self.collator);
            if start < end {
                let row = &rows[if reverse { end - 1 } else { start }];
                return Ok(Some(read(row)));
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
        let range = range.borrow().as_ref();
        range.validate(self.schema.len(), &self.collator)?;
        self.edge(&range, false, |_| ())
            .await
            .map(|row| row.is_none())
    }
}

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync,
    Node<S::Value>: fmt::Debug,
{
    /// Visit matching rows in order without cloning their values.
    ///
    /// The synchronous callback may borrow caller-owned data. It cannot retain
    /// a row or reacquire storage protected by this tree owner. Returning
    /// [`ControlFlow::Break`] stops the scan. Completion, error, cancellation,
    /// and early termination release all scan-owned guards.
    pub async fn visit<BV, E>(
        &self,
        range: Range<BV>,
        reverse: bool,
        mut visit: impl FnMut(&[S::Value]) -> Result<ControlFlow<()>, E> + Send,
    ) -> Result<(), E>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync,
        E: From<io::Error>,
    {
        range.validate(self.schema.len(), &self.collator)?;
        let mut nodes = nodes(
            &self.dir,
            self.collator.clone(),
            range,
            ROOT,
            reverse,
            Vec::new(),
            &*self.schema,
        )
        .await?;

        while let Some(leaf) = nodes.try_next().await? {
            let rows = leaf.as_ref();
            for offset in 0..rows.len() {
                let index = if reverse {
                    rows.len() - offset - 1
                } else {
                    offset
                };
                let row = &rows[index];
                if visit(row)?.is_break() {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Construct a [`Stream`] of all the keys in the given `range` of this B+Tree.
    pub async fn keys<'a, BV>(self, range: Range<BV>) -> Result<ScopedKeys<'a, S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'a,
        G: 'a,
        S: 'a,
    {
        self.scan(range, None, false).await
    }

    pub async fn keys_rev<'a, BV>(
        self,
        range: Range<BV>,
    ) -> Result<ScopedKeys<'a, S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'a,
        G: 'a,
        S: 'a,
    {
        self.scan(range, None, true).await
    }

    /// Stream selected columns in their requested order, retaining every matching row.
    /// Range filtering uses complete stored keys; only selected values are cloned.
    pub async fn select<'a, BV>(
        self,
        range: Range<BV>,
        columns: Vec<usize>,
        reverse: bool,
    ) -> Result<ScopedKeys<'a, S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'a,
        G: 'a,
        S: 'a,
    {
        self.scan(range, Some(columns), reverse).await
    }

    async fn scan<'a, BV>(
        self,
        range: Range<BV>,
        columns: Option<Vec<usize>>,
        reverse: bool,
    ) -> Result<ScopedKeys<'a, S::Value>, io::Error>
    where
        BV: Borrow<S::Value> + Clone + Send + Sync + 'a,
        G: 'a,
        S: 'a,
    {
        range.validate(self.schema.len(), &self.collator)?;
        if columns
            .as_ref()
            .is_some_and(|columns| columns.iter().any(|&column| column >= self.schema.len()))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "selected column is outside the BTree schema",
            ));
        }

        let nodes = nodes(
            self.dir,
            self.collator,
            range,
            ROOT,
            reverse,
            Vec::new(),
            self.schema,
        )
        .await?;
        let keys = nodes
            .map(move |leaf| {
                let leaf = leaf?;
                // Rows already contain inline key storage; keep the outer batch on the
                // heap to avoid oversized temporaries when polling stream combinators.
                let mut keys: Vec<Key<S::Value>> = Vec::with_capacity(leaf.as_ref().len());

                for offset in 0..leaf.as_ref().len() {
                    let index = if reverse {
                        leaf.as_ref().len() - offset - 1
                    } else {
                        offset
                    };
                    let row = &leaf.as_ref()[index];
                    let key = if let Some(columns) = &columns {
                        columns.iter().map(|&column| row[column].clone()).collect()
                    } else {
                        row.iter().cloned().collect()
                    };
                    keys.push(key);
                }

                Ok::<_, io::Error>(stream::iter(keys).map(Ok))
            })
            .try_flatten();

        Ok(Box::pin(keys))
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

fn nodes<'a, S, C, BV, FE, G, H>(
    dir: G,
    collator: Collator<C>,
    range: Range<BV>,
    node_id: Uuid,
    reverse: bool,
    mut ancestors: Vec<Uuid>,
    schema: H,
) -> Pin<Box<dyn Future<Output = ResultNodes<'a, FE, S::Value>> + Send + 'a>>
where
    S: Schema + Send + Sync + 'a,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    H: Deref<Target = S> + Clone + Send + Sync + 'a,
    BV: Borrow<S::Value> + Clone + Send + Sync + 'a,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'a,
{
    #[cfg(feature = "logging")]
    log::debug!("reading BTree keys (reverse: {reverse})");

    if ancestors.contains(&node_id) {
        return Box::pin(future::ready(Err(freqfs::IntegrityError::invalid_data(
            "cycle in BTree ancestry",
        ))));
    }
    ancestors.push(node_id);

    let Some(file) = dir
        .as_dir()
        .get_file(
            node_id
                .hyphenated()
                .encode_lower(&mut Uuid::encode_buffer()),
        )
        .cloned()
    else {
        // A referenced child (or the required root) must exist in this native tree.
        return Box::pin(future::ready(Err(freqfs::IntegrityError::mark(
            io::Error::new(io::ErrorKind::NotFound, node_id.to_string()),
        ))));
    };

    let fut = file.into_read().map(move |node| {
        let node = node.map_err(super::required_node_error)?;
        let read = match &*node {
            Node::Leaf(keys) => {
                validate_rows(keys, schema.len())?;
                if range.is_empty() {
                    NodeRead::Leaf((0, keys.len()))
                } else {
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
            }
            Node::Index(separators, children) => {
                validate_index(&*schema, &collator, separators, children)?;
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

        let nodes: Nodes<'a, FE, S::Value> = match read {
            NodeRead::Excluded => {
                let nodes = stream::empty();
                Box::pin(nodes)
            }
            NodeRead::Child(node_id) => {
                let nodes = stream::once(nodes(
                    dir, collator, range, node_id, reverse, ancestors, schema,
                ))
                .try_flatten();

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
                            ancestors.clone(),
                            schema.clone(),
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
#[inline]
fn stack_key<'a, T, A>(iter: A) -> SmallVec<[T; NODE_STACK_SIZE]>
where
    T: Clone + 'a,
    A: IntoIterator<Item = &'a T>,
{
    iter.into_iter().cloned().collect()
}
