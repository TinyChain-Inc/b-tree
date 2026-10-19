//! Native tree ownership and shared node access.
//!
//! Reading, insertion, deletion, replacement, and decoding live in private
//! operation modules.

use std::collections::HashSet;
use std::sync::Arc;
use std::{fmt, io, pin::Pin};

use collate::Collate;
use freqfs::{
    DirDeref, DirLock, DirReadGuardOwned, DirWriteGuardOwned, FileLoad, FileReadGuardOwned,
    FileSave, FileWriteGuardOwned,
};
use futures::{FutureExt, Stream};
use safecast::AsType;
use uuid::Uuid;

use super::{Collator, Key, Schema};

#[cfg(feature = "stream")]
mod codec;
mod delete;
mod insert;
mod read;
mod replace;

/// A read guard acquired on a [`BTreeLock`]
pub type BTreeReadGuard<S, C, FE> = BTree<S, C, Arc<DirReadGuardOwned<FE>>>;

/// A write guard acquired on a [`BTreeLock`]
pub type BTreeWriteGuard<S, C, FE> = BTree<S, C, DirWriteGuardOwned<FE>>;

/// A stream of [`Key`]s in a [`BTree`]
pub type Keys<V> = Pin<Box<dyn Stream<Item = Result<Key<V>, io::Error>> + Send>>;

type Node<V> = super::node::Node<Vec<Vec<V>>>;

const ROOT: Uuid = Uuid::from_fields(0, 0, 0, &[0u8; 8]);

/// A read guard on the content of a leaf node
pub struct Leaf<FE, V> {
    node: FileReadGuardOwned<FE, Node<V>>,
    range: (usize, usize),
}

impl<FE, V> Leaf<FE, V> {
    fn new(node: FileReadGuardOwned<FE, Node<V>>, range: (usize, usize)) -> Self {
        debug_assert!(node.is_leaf());
        Self { node, range }
    }
}

impl<FE, V> AsRef<[Vec<V>]> for Leaf<FE, V> {
    fn as_ref(&self) -> &[Vec<V>] {
        match &*self.node {
            Node::Leaf(keys) => &keys[self.range.0..self.range.1],
            _ => panic!("not a leaf!"),
        }
    }
}

/// A futures-aware read-write lock on a [`BTree`]
pub struct BTreeLock<S, C, FE> {
    schema: Arc<S>,
    collator: Collator<C>,
    dir: DirLock<FE>,
}

impl<S, C, FE> Clone for BTreeLock<S, C, FE>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir: self.dir.clone(),
        }
    }
}

impl<S, C, FE> BTreeLock<S, C, FE> {
    /// Borrow the [`Collator`] used by this B+Tree.
    pub fn collator(&self) -> &Collator<C> {
        &self.collator
    }

    /// Borrow the schema of the source B+Tree.
    pub fn schema(&self) -> &S {
        &self.schema
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    S: Schema,
    FE: AsType<Node<S::Value>> + From<Node<S::Value>> + Send + Sync + FileLoad,
{
    fn new(schema: S, collator: C, dir: DirLock<FE>) -> Self {
        Self {
            schema: Arc::new(schema),
            collator: Collator::new(collator),
            dir,
        }
    }

    /// Create a new [`BTreeLock`], awaiting admission of its root in `dir`.
    pub async fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        validate_order(schema.order())?;
        let mut nodes = dir.try_write_owned()?;

        if nodes.is_empty() {
            nodes
                .create_empty_file::<Node<S::Value>>(ROOT.to_string(), Node::Leaf(vec![]))
                .await?;

            debug_assert!(nodes.contains(&ROOT), "B+Tree failed to create a root node");

            Ok(Self::new(schema, collator, dir))
        } else {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "creating a new B+Tree requires an empty file",
            ))
        }
    }

    /// Load a [`BTreeLock`] with the given `schema` and `collator` from `dir`.
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        validate_order(schema.order())?;
        {
            let nodes = dir.try_read()?;
            if !nodes.contains(&ROOT) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing B+Tree root",
                ));
            }
        }

        Ok(Self::new(schema, collator, dir))
    }

    /// Write any modified blocks of thie [`BTree`] to the filesystem.
    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: FileSave + Clone,
    {
        self.dir.sync().await
    }

    /// Explicitly make canonical storage durable without publishing pending versions.
    pub async fn sync_all(&self) -> Result<(), io::Error>
    where
        FE: FileSave + Clone,
    {
        self.dir.sync_all().await
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    C: Clone,
    FE: Send + Sync,
{
    /// Lock this B+Tree for reading, without borrowing.
    pub async fn into_read(self) -> BTreeReadGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for reading...", self.dir);

        self.dir
            .into_read()
            .map(Arc::new)
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for reading.
    pub async fn read(&self) -> BTreeReadGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for reading...", self.dir);

        self.dir
            .read_owned()
            .map(Arc::new)
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<BTreeReadGuard<S, C, FE>, io::Error> {
        self.dir.try_read_owned().map(Arc::new).map(|dir| BTree {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir,
        })
    }

    /// Lock this B+Tree for writing, without borrowing.
    pub async fn into_write(self) -> BTreeWriteGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for writing...", self.dir);

        self.dir
            .into_write()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for writing.
    pub async fn write(&self) -> BTreeWriteGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for writing...", self.dir);

        self.dir
            .write_owned()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<BTreeWriteGuard<S, C, FE>, io::Error> {
        self.dir.try_write_owned().map(|dir| BTree {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir,
        })
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Validate the complete selected tree before accepting storage.
    pub async fn validate(&self) -> Result<(), io::Error> {
        let invalid = |message| io::Error::new(io::ErrorKind::InvalidData, message);
        let tree = self.read().await;
        for (name, entry) in tree.dir.as_dir().iter() {
            if name.parse::<Uuid>().is_err() || !entry.is_file() {
                return Err(invalid("invalid BTree node entry"));
            }
        }

        let mut seen = HashSet::new();
        let mut pending = vec![(ROOT, None::<Vec<S::Value>>, None::<Vec<S::Value>>, 0)];
        let mut leaf_depth = None;
        let mut has_ties = false;

        while let Some((id, lower, upper, depth)) = pending.pop() {
            if !seen.insert(id) {
                return Err(invalid("repeated BTree node"));
            }

            let node = tree
                .dir
                .as_dir()
                .read_file::<_, Node<S::Value>>(&id)
                .await?;

            // Collators only receive columns accepted by the owning schema.
            let keys = match &*node {
                Node::Leaf(rows) => {
                    let order = self.schema.order();
                    let valid_count = if id == ROOT {
                        rows.len() <= order
                    } else {
                        rows.len() >= order / 2 && rows.len() < order
                    };

                    if !valid_count
                        || rows.iter().any(|row| {
                            row.len() != self.schema.len()
                                || self.schema.validate_key(row.clone()).is_err()
                        })
                    {
                        return Err(invalid("invalid BTree leaf"));
                    }

                    rows
                }
                Node::Index(separators, children) => {
                    validate_children(separators, children)?;
                    if children.len() > self.schema.order()
                        || (id != ROOT && children.len() < self.schema.order().div_ceil(2))
                        || separators
                            .iter()
                            .any(|prefix| self.schema.validate_prefix(prefix).is_err())
                    {
                        return Err(invalid("invalid BTree separators"));
                    }

                    separators
                }
            };

            if keys
                .windows(2)
                .any(|pair| self.collator.cmp_fences(&pair[0], &pair[1]).is_gt())
                || lower.as_ref().is_some_and(|lower| {
                    keys.first()
                        .is_some_and(|first| self.collator.cmp_fences(first, lower).is_lt())
                })
                || upper.as_ref().is_some_and(|upper| {
                    keys.last()
                        .is_some_and(|last| self.collator.cmp_fences(last, upper).is_gt())
                })
            {
                return Err(invalid("invalid BTree ordering or subtree boundary"));
            }

            match &*node {
                Node::Leaf(rows) => {
                    has_ties |= rows
                        .windows(2)
                        .any(|pair| self.collator.cmp(&pair[0], &pair[1]).is_eq())
                        || upper.as_ref().is_some_and(|upper| {
                            rows.last()
                                .is_some_and(|row| self.collator.cmp_fences(row, upper).is_eq())
                        });

                    if leaf_depth
                        .replace(depth)
                        .is_some_and(|prior| prior != depth)
                    {
                        return Err(invalid("unbalanced BTree"));
                    }
                }
                Node::Index(separators, children) => {
                    for (i, child) in children.iter().enumerate() {
                        pending.push((
                            *child,
                            i.checked_sub(1)
                                .map(|j| separators[j].clone())
                                .or_else(|| lower.clone()),
                            separators.get(i).cloned().or_else(|| upper.clone()),
                            depth + 1,
                        ));
                    }
                }
            }
        }

        // Validate every structural path before identity lookup follows any of
        // them. Otherwise a tied leaf could start a lookup through a corrupt
        // sibling cycle before the structural traversal reaches that sibling.
        if !has_ties {
            return Ok(());
        }

        let mut pending = vec![(ROOT, None::<Vec<S::Value>>)];

        while let Some((id, upper)) = pending.pop() {
            let node = tree
                .dir
                .as_dir()
                .read_file::<_, Node<S::Value>>(&id)
                .await?;

            match &*node {
                Node::Index(separators, children) => {
                    for (i, child) in children.iter().enumerate() {
                        pending
                            .push((*child, separators.get(i).cloned().or_else(|| upper.clone())));
                    }
                }
                Node::Leaf(rows) => {
                    for (i, row) in rows.iter().enumerate() {
                        // Exact identity remains independent of collation. Checking
                        // each tied row is quadratic in a large equivalent group,
                        // without collecting that group or counting routing copies.
                        let tied = rows
                            .get(i + 1)
                            .is_some_and(|next| self.collator.cmp(row, next).is_eq())
                            || i.checked_sub(1)
                                .is_some_and(|j| self.collator.cmp(row, &rows[j]).is_eq())
                            || upper
                                .as_ref()
                                .is_some_and(|upper| self.collator.cmp_fences(row, upper).is_eq());

                        if tied {
                            tree.get(row).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Copy a caller-coordinated live tree into empty, unpublished delegated storage.
    /// The source remains read-locked throughout copying; this does not validate or
    /// repair it. Failure may leave an incomplete unpublished destination.
    pub async fn copy_into(&self, target: DirLock<FE>) -> Result<Self, io::Error>
    where
        FE: Clone,
    {
        {
            let mut target = target.write().await;
            if !target.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "copy requires empty storage",
                ));
            }
            // The empty target is exclusively owned construction storage. Check
            // it before locking the source so invalid reverse copies cannot deadlock.
            let source = self.dir.read().await;
            for (name, entry) in source.iter() {
                target
                    .copy_file_from(
                        name.clone(),
                        entry.as_file().ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid BTree node entry")
                        })?,
                    )
                    .await?;
            }
        }

        Ok(Self {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir: target,
        })
    }
}

/// A B+Tree
pub struct BTree<S, C, G> {
    schema: Arc<S>,
    collator: Collator<C>,
    dir: G,
}

impl<S, C, G> Clone for BTree<S, C, G>
where
    C: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir: self.dir.clone(),
        }
    }
}

impl<S, C, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value>,
{
    /// Borrow the [`Schema`] of this [`BTree`].
    pub fn schema(&self) -> &S {
        &self.schema
    }
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>> {
    /// Downgrade this [`BTreeWriteGuard`] into a [`BTreeReadGuard`].
    pub fn downgrade(self) -> BTreeReadGuard<S, C, FE> {
        BTreeReadGuard {
            schema: self.schema,
            collator: self.collator,
            dir: Arc::new(self.dir.downgrade()),
        }
    }
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    async fn write_node(
        &self,
        id: &Uuid,
    ) -> Result<FileWriteGuardOwned<FE, Node<S::Value>>, io::Error> {
        self.dir.write_file_owned(id).await
    }
}

impl<S: fmt::Debug, C, G> fmt::Debug for BTree<S, C, G> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "B+Tree index with schema {:?}", self.schema)
    }
}

#[inline]
fn validate_key<S: Schema>(schema: &S, key: Vec<S::Value>) -> Result<Vec<S::Value>, io::Error> {
    schema
        .validate_key(key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
}

#[inline]
fn validate_collator_eq<S>(this: &S, that: &S) -> Result<(), io::Error>
where
    S: PartialEq,
{
    if this == that {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "B+Tree to merge must have the same collation",
        ))
    }
}

#[inline]
fn validate_schema_eq<S>(this: &S, that: &S) -> Result<(), io::Error>
where
    S: PartialEq + fmt::Debug,
{
    if this == that {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "cannot merge a B+Tree with schema {:?} into one with schema {:?}",
                that, this
            ),
        ))
    }
}

// Internal nodes contain one routing fence between each pair of children.
// In particular, this rejects the former child-minimum representation.
fn validate_children<V>(separators: &[Vec<V>], children: &[Uuid]) -> io::Result<()> {
    if children.len() < 2 || separators.len() != children.len() - 1 {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid BTree children",
        ))
    } else {
        Ok(())
    }
}

fn validate_order(order: usize) -> io::Result<()> {
    if order < 3 {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "BTree order must be at least three",
        ))
    } else {
        Ok(())
    }
}
