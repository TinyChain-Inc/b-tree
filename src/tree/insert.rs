//! Ordinary and ordered insertion share splitting and ancestor propagation.

use std::{io, mem, pin::Pin};

use collate::Collate;
use freqfs::{DirDeref, DirWriteGuardOwned, FileLoad, FileReadGuard, FileWriteGuardOwned};
use futures::future::Future;
use futures::stream::{Stream, TryStreamExt};
use safecast::AsType;
use uuid::Uuid;

use super::{BTree, Node, ROOT, Schema, validate_collator_eq, validate_key, validate_schema_eq};
use crate::node::Block;
use crate::range::Range;

type ResultInsert<V> = Result<Insert<V>, io::Error>;

enum Insert<V> {
    None,
    Left(Vec<V>),
    Right,
    OverflowLeft(Vec<V>, Vec<V>, Uuid),
    Overflow(Vec<V>, Uuid),
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Return `false` if te given `key` is already present.
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
                    if self.collator.cmp_slices(&key, previous).is_lt() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "ordered insertion precedes the current maximum",
                        ));
                    }

                    if previous == &key {
                        continue;
                    }
                }

                previous = Some(key.clone());

                {
                    let mut leaf = self.write_node(&leaf_id).await?;
                    let Node::Leaf(rows) = &*leaf else {
                        unreachable!("rightmost leaf")
                    };

                    // Leave splitting and empty-root initialization to insert_root.
                    // Appending to a nonempty leaf never changes an ancestor bound.
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
                Node::Index(_, children) => {
                    id = *children.last().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "empty BTree index")
                    })?;
                }
            }
        }
    }

    async fn create_node(
        &mut self,
        node: Node<S::Value>,
    ) -> Result<(Uuid, freqfs::FileLock<FE>), io::Error> {
        self.dir.create_file_unique(node).await
    }

    async fn insert_root(&mut self, key: Vec<S::Value>) -> Result<bool, io::Error> {
        let order = self.schema.order();

        let mut root = self.write_node(&ROOT).await?;
        let new_root = match &*root {
            Node::Leaf(keys) => {
                let i = keys.bisect_left(&key, &self.collator);

                if keys.get(i) == Some(&key) {
                    // no-op
                    return Ok(false);
                }

                let Node::Leaf(keys) = &mut *root else {
                    unreachable!("leaf")
                };
                keys.insert(i, key);

                if keys.len() > order {
                    let mid = order.div_ceil(2);

                    let right: Vec<_> = keys.drain(mid..).collect();
                    debug_assert!(right.len() >= mid);

                    let right_key = right[0].clone();
                    let (right, _) = self.create_node(Node::Leaf(right)).await?;

                    let left: Vec<_> = mem::take(keys);
                    debug_assert!(left.len() >= mid);

                    let left_key = left[0].clone();
                    let (left, _) = self.create_node(Node::Leaf(left)).await?;

                    Some(Node::Index(vec![left_key, right_key], vec![left, right]))
                } else {
                    None
                }
            }
            Node::Index(bounds, children) => {
                debug_assert_eq!(bounds.len(), children.len());

                // Equality belongs to this child, whose first key is the bound.
                let i = match bounds.bisect_right(&key, &self.collator) {
                    0 => 0,
                    i => i - 1,
                };

                let mut child = self.write_node(&children[i]).await?;
                let result = self.insert_inner(&mut child, key).await?;
                let Node::Index(bounds, children) = &mut *root else {
                    unreachable!("index")
                };

                match result {
                    Insert::None => return Ok(false),
                    Insert::Right => {}
                    Insert::Left(key) => {
                        bounds[i] = key;
                    }
                    Insert::OverflowLeft(left, middle, child_id) => {
                        bounds[i] = left;
                        bounds.insert(i + 1, middle);
                        children.insert(i + 1, child_id);
                    }
                    Insert::Overflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);
                    }
                }

                // debug_assert!(self.collator.is_sorted(&bounds));
                debug_assert_eq!(bounds.len(), children.len());

                if children.len() > order {
                    let right_bounds: Vec<_> = bounds.drain(order.div_ceil(2)..).collect();
                    let right_children: Vec<_> = children.drain(order.div_ceil(2)..).collect();
                    let right_bound = right_bounds[0].clone();
                    let (right_node_id, _) = self
                        .create_node(Node::Index(right_bounds, right_children))
                        .await?;

                    let left_bounds: Vec<_> = mem::take(bounds);
                    let left_children: Vec<_> = mem::take(children);
                    let left_bound = left_bounds[0].clone();
                    let (left_node_id, _) = self
                        .create_node(Node::Index(left_bounds, left_children))
                        .await?;

                    Some(Node::Index(
                        vec![left_bound, right_bound],
                        vec![left_node_id, right_node_id],
                    ))
                } else {
                    None
                }
            }
        };

        if let Some(new_root) = new_root {
            *root = new_root;
        }

        Ok(true)
    }

    fn insert_inner<'a>(
        &'a mut self,
        node: &'a mut FileWriteGuardOwned<FE, Node<S::Value>>,
        key: Vec<S::Value>,
    ) -> Pin<Box<dyn Future<Output = ResultInsert<S::Value>> + Send + 'a>> {
        Box::pin(async move {
            let order = self.schema.order();
            match &**node {
                Node::Leaf(keys) => {
                    let i = keys.bisect_left(&key, &self.collator);

                    if i < keys.len() && keys[i] == key {
                        // no-op
                        return Ok(Insert::None);
                    }

                    let Node::Leaf(keys) = &mut **node else {
                        unreachable!("leaf")
                    };
                    keys.insert(i, key);

                    let mid = order / 2;

                    if keys.len() >= order {
                        let new_leaf: Vec<_> = keys.drain(mid..).collect();

                        debug_assert!(new_leaf.len() >= mid);
                        debug_assert!(keys.len() >= mid);

                        let middle_key = new_leaf[0].to_vec();
                        let node = Node::Leaf(new_leaf);
                        let (new_node_id, _) = self.create_node(node).await?;

                        if i == 0 {
                            Ok(Insert::OverflowLeft(
                                keys[0].to_vec(),
                                middle_key,
                                new_node_id,
                            ))
                        } else {
                            Ok(Insert::Overflow(middle_key, new_node_id))
                        }
                    } else {
                        debug_assert!(keys.len() > mid);

                        if i == 0 {
                            Ok(Insert::Left(keys[0].to_vec()))
                        } else {
                            Ok(Insert::Right)
                        }
                    }
                }
                Node::Index(bounds, children) => {
                    debug_assert_eq!(bounds.len(), children.len());

                    // Equality belongs to this child, whose first key is the bound.
                    let i = match bounds.bisect_right(&key, &self.collator) {
                        0 => 0,
                        i => i - 1,
                    };

                    let mut child = self.write_node(&children[i]).await?;

                    let result = self.insert_inner(&mut child, key).await?;
                    let Node::Index(bounds, children) = &mut **node else {
                        unreachable!("index")
                    };
                    let overflow_left = match result {
                        Insert::None => return Ok(Insert::None),
                        Insert::Right => return Ok(Insert::Right),
                        Insert::Left(key) => {
                            bounds[i] = key;

                            return if i == 0 {
                                Ok(Insert::Left(bounds[i].to_vec()))
                            } else {
                                Ok(Insert::Right)
                            };
                        }
                        Insert::OverflowLeft(left, middle, child_id) => {
                            bounds[i] = left;
                            bounds.insert(i + 1, middle);
                            children.insert(i + 1, child_id);
                            i == 0
                        }
                        Insert::Overflow(bound, child_id) => {
                            bounds.insert(i + 1, bound);
                            children.insert(i + 1, child_id);
                            false
                        }
                    };

                    // debug_assert!(self.collator.is_sorted(bounds));

                    if children.len() > order {
                        let mid = self.schema.order().div_ceil(2);
                        let new_bounds: Vec<_> = bounds.drain(mid..).collect();
                        let new_children: Vec<_> = children.drain(mid..).collect();

                        let left_bound = new_bounds[0].to_vec();
                        let node = Node::Index(new_bounds, new_children);
                        let (node_id, _) = self.create_node(node).await?;

                        if overflow_left {
                            Ok(Insert::OverflowLeft(
                                bounds[0].to_vec(),
                                left_bound,
                                node_id,
                            ))
                        } else {
                            Ok(Insert::Overflow(left_bound, node_id))
                        }
                    } else if i == 0 {
                        Ok(Insert::Left(bounds[0].to_vec()))
                    } else {
                        Ok(Insert::Right)
                    }
                }
            }
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
