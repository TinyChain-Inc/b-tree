//! Ordinary and ordered insertion share splitting and ancestor propagation.

use std::cmp::Ordering;
use std::{io, mem, pin::Pin};

use collate::Collate;
use freqfs::{DirDeref, DirWriteGuardOwned, FileLoad, FileReadGuard, FileWriteGuardOwned};
use futures::future::Future;
use futures::stream::{Stream, TryStreamExt};
use safecast::AsType;
use uuid::Uuid;

use super::{BTree, Node, ROOT, Schema, validate_collator_eq, validate_key, validate_schema_eq};
use crate::range::Range;

type ResultInsert<V> = Result<Insert<V>, io::Error>;

enum Insert<V> {
    Complete,
    Split(Vec<V>, Uuid),
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Return `false` if the given `key` is already present.
    pub async fn insert(&mut self, key: Vec<S::Value>) -> Result<bool, io::Error> {
        let key = validate_key(&*self.schema, key)?;
        self.insert_root(key).await
    }

    /// Append an ordered stream of keys at or beyond the current maximum.
    /// Equal keys are ignored, as with `insert`. Returns the number inserted.
    /// Errors and cancellation retain the successfully inserted prefix.
    /// Only a rightmost leaf identifier and one previous key are retained;
    /// splits and ancestor updates use the ordinary insertion implementation.
    pub fn insert_sorted<'a, K>(
        &'a mut self,
        keys: K,
    ) -> Pin<Box<dyn Future<Output = Result<u64, io::Error>> + Send + 'a>>
    where
        K: Stream<Item = Result<Vec<S::Value>, io::Error>> + Send + 'a,
    {
        Box::pin(async move {
            futures::pin_mut!(keys);

            // A split can leave a different collator-equal row at the right edge.
            // Retain the last input to preserve exact duplicate detection.
            let (mut leaf_id, mut previous) = {
                let (id, leaf) = self.rightmost_leaf().await?;
                let previous = match &*leaf {
                    Node::Leaf(rows) => rows.last().cloned(),
                    Node::Index(..) => unreachable!("rightmost leaf"),
                };

                (id, previous)
            };
            let mut inserted = 0;

            while let Some(key) = keys.try_next().await? {
                let key = validate_key(&*self.schema, key)?;

                if let Some(previous) = &previous {
                    match self.collator.cmp_slices(&key, previous) {
                        Ordering::Less => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "ordered insertion precedes the current maximum",
                            ));
                        }
                        Ordering::Equal if self.contains(&key).await? => continue,
                        _ => {}
                    }
                }

                previous = Some(key.clone());

                {
                    let mut leaf = self.write_node(&leaf_id).await?;
                    let Node::Leaf(rows) = &*leaf else {
                        unreachable!("rightmost leaf")
                    };

                    // Leave splitting and empty-root initialization to insert_root.
                    // Appending to a nonempty leaf never changes a routing separator.
                    let limit = if leaf_id == ROOT {
                        self.schema.order()
                    } else {
                        self.schema.order() - 1
                    };

                    if !rows.is_empty() && rows.len() < limit {
                        let Node::Leaf(rows) = &mut *leaf else {
                            unreachable!("rightmost leaf")
                        };
                        rows.push(key);
                        inserted += 1;
                        continue;
                    }
                }

                inserted += u64::from(self.insert_root(key).await?);
                leaf_id = self.rightmost_leaf().await?.0;
            }

            Ok(inserted)
        })
    }

    async fn rightmost_leaf(&self) -> Result<(Uuid, FileReadGuard<'_, Node<S::Value>>), io::Error> {
        let mut id = ROOT;

        loop {
            let node = self.dir.read_file::<_, Node<S::Value>>(&id).await?;

            match &*node {
                Node::Leaf(_) => return Ok((id, node)),
                Node::Index(separators, children) => {
                    super::validate_children(separators, children)?;
                    id = *children.last().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "empty BTree index")
                    })?;
                }
            }
        }
    }

    async fn insert_root(&mut self, key: Vec<S::Value>) -> Result<bool, io::Error> {
        let (path, present) = self.locate(&key).await?;
        if present {
            return Ok(false);
        }

        let mut root = self.write_node(&ROOT).await?;
        if let Insert::Split(separator, right) = self.insert_inner(&mut root, key, &path).await? {
            let left = mem::replace(&mut *root, Node::Leaf(Vec::new()));
            let (left, _) = self.dir.create_file_unique(left).await?;
            *root = Node::Index(vec![separator], vec![left, right]);
        }

        Ok(true)
    }

    fn insert_inner<'a>(
        &'a mut self,
        node: &'a mut FileWriteGuardOwned<FE, Node<S::Value>>,
        key: Vec<S::Value>,
        path: &'a [(Uuid, usize)],
    ) -> Pin<Box<dyn Future<Output = ResultInsert<S::Value>> + Send + 'a>> {
        Box::pin(async move {
            let order = self.schema.order();
            let slot = path[0].1;
            let (separator, right) = match &mut **node {
                Node::Leaf(rows) => {
                    rows.insert(slot, key);
                    let root = path[0].0 == ROOT;
                    if rows.len() < order || (root && rows.len() == order) {
                        return Ok(Insert::Complete);
                    }

                    let mid = if root { order.div_ceil(2) } else { order / 2 };
                    let right = rows.split_off(mid);
                    let separator = self
                        .collator
                        .separator(rows.last().expect("split left"), &right[0]);

                    (separator, Node::Leaf(right))
                }
                Node::Index(separators, children) => {
                    super::validate_children(separators, children)?;
                    let mut child = self.write_node(&children[slot]).await?;
                    let result = self.insert_inner(&mut child, key, &path[1..]).await?;
                    drop(child);

                    let Insert::Split(separator, right) = result else {
                        return Ok(Insert::Complete);
                    };

                    separators.insert(slot, separator);
                    children.insert(slot + 1, right);
                    if children.len() <= order {
                        return Ok(Insert::Complete);
                    }

                    // The boundary between the resulting child groups belongs
                    // only to their parent, not to either split index.
                    let mid = order.div_ceil(2);
                    let right_children = children.split_off(mid);
                    let right_separators = separators.split_off(mid);
                    let separator = separators.pop().expect("split boundary");

                    (separator, Node::Index(right_separators, right_children))
                }
            };

            let (right, _) = self.dir.create_file_unique(right).await?;
            Ok(Insert::Split(separator, right))
        })
    }
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
{
    /// Merge the keys from the `other` B+Tree range into this one.
    ///
    /// The source B+Tree **must** have an identical schema and collation.
    pub async fn merge<G>(&mut self, other: BTree<S, C, G>) -> Result<(), io::Error>
    where
        G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        validate_collator_eq(&self.collator, &other.collator)?;
        validate_schema_eq(&self.schema, &other.schema)?;

        let mut keys = other.keys(Range::<S::Value>::default()).await?;
        while let Some(key) = keys.try_next().await? {
            self.insert_root(key.into_vec()).await?;
        }

        Ok(())
    }
}
