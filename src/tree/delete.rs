//! Deletion owns borrowing, merging, and root collapse.

use std::borrow::Borrow;
use std::{io, mem, pin::Pin};

use collate::Collate;
use freqfs::{DirDeref, DirWriteGuardOwned, FileLoad, FileWriteGuardOwned};
use futures::TryStreamExt;
use futures::future::Future;
use safecast::AsType;
use uuid::Uuid;

use super::{
    BTree, Node, ROOT, Schema, validate_children, validate_collator_eq, validate_schema_eq,
};
use crate::range::Range;

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Delete the given `key` from this B+Tree.
    pub async fn delete<V>(&mut self, key: &[V]) -> Result<bool, io::Error>
    where
        V: Borrow<S::Value> + Send + Sync,
    {
        let (path, present) = self.locate(key).await?;
        if !present {
            return Ok(false);
        }

        let mut root = self.write_node(&ROOT).await?;
        if !self.delete_inner(&mut root, key, &path).await? {
            return Ok(false);
        }

        let only_child = match &*root {
            Node::Index(separators, children) if children.len() == 1 => {
                if !separators.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid root separators",
                    ));
                }

                Some(children[0])
            }
            _ => None,
        };

        if let Some(id) = only_child {
            let contents = {
                let mut child = self.write_node(&id).await?;
                match &mut *child {
                    Node::Leaf(rows) => Node::Leaf(mem::take(rows)),
                    Node::Index(separators, children) => {
                        Node::Index(mem::take(separators), mem::take(children))
                    }
                }
            };

            self.dir.delete(&id).await;
            *root = contents;
        }

        Ok(true)
    }

    fn delete_inner<'a, V>(
        &'a mut self,
        node: &'a mut FileWriteGuardOwned<FE, Node<S::Value>>,
        key: &'a [V],
        path: &'a [(Uuid, usize)],
    ) -> Pin<Box<dyn Future<Output = Result<bool, io::Error>> + Send + 'a>>
    where
        V: Borrow<S::Value> + Send + Sync,
    {
        Box::pin(async move {
            let slot = path[0].1;
            match &mut **node {
                Node::Leaf(rows) => {
                    if rows
                        .get(slot)
                        .is_some_and(|row| row.iter().zip(key).all(|(a, b)| a == b.borrow()))
                    {
                        rows.remove(slot);
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
                Node::Index(separators, children) => {
                    validate_children(separators, children)?;
                    let mut child = self.write_node(&children[slot]).await?;
                    if !self.delete_inner(&mut child, key, &path[1..]).await? {
                        return Ok(false);
                    }

                    let underflow = match &*child {
                        Node::Leaf(rows) => rows.len() < self.schema.order() / 2,
                        Node::Index(_, children) => {
                            children.len() < self.schema.order().div_ceil(2)
                        }
                    };
                    let retired = if underflow {
                        self.rebalance(&mut child, slot, separators, children)
                            .await?
                    } else {
                        None
                    };

                    drop(child);
                    if let Some(id) = retired {
                        self.dir.delete(&id).await;
                    }

                    // Removing a leaf value does not invalidate a routing separator.
                    Ok(true)
                }
            }
        })
    }

    async fn rebalance(
        &self,
        child: &mut Node<S::Value>,
        slot: usize,
        separators: &mut Vec<Vec<S::Value>>,
        children: &mut Vec<Uuid>,
    ) -> io::Result<Option<Uuid>> {
        let boundary = slot.saturating_sub(1);
        let sibling_slot = if slot == 0 { 1 } else { slot - 1 };
        let mut sibling = self.write_node(&children[sibling_slot]).await?;
        let (left, right) = if slot == 0 {
            (child, &mut *sibling)
        } else {
            (&mut *sibling, child)
        };

        match (left, right) {
            (Node::Leaf(left), Node::Leaf(right)) => {
                let available = if slot == 0 { right.len() } else { left.len() };
                if available > self.schema.order() / 2 {
                    if slot == 0 {
                        left.push(right.remove(0));
                    } else {
                        right.insert(0, left.pop().expect("nonempty lending leaf"));
                    }

                    separators[boundary] = self.collator.separator(
                        left.last().expect("nonempty left leaf"),
                        right.first().expect("nonempty right leaf"),
                    );
                    return Ok(None);
                }

                left.append(right);
                separators.remove(boundary);
            }
            (Node::Index(left, left_children), Node::Index(right, right_children)) => {
                if left.len() + 1 != left_children.len() || right.len() + 1 != right_children.len()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid sibling separators",
                    ));
                }

                let available = if slot == 0 {
                    right_children.len()
                } else {
                    left_children.len()
                };
                if available > self.schema.order().div_ceil(2) {
                    // Internal redistribution rotates the intervening parent separator.
                    if slot == 0 {
                        let promoted = right.remove(0);
                        left.push(mem::replace(&mut separators[boundary], promoted));
                        left_children.push(right_children.remove(0));
                    } else {
                        let promoted = left.pop().expect("nonempty lending index");
                        right.insert(0, mem::replace(&mut separators[boundary], promoted));
                        right_children
                            .insert(0, left_children.pop().expect("nonempty lending index"));
                    }
                    return Ok(None);
                }

                left.push(separators.remove(boundary));
                left.append(right);
                left_children.append(right_children);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unequal sibling depths",
                ));
            }
        }

        Ok(Some(children.remove(boundary + 1)))
    }

    /// Delete all the keys in this [`BTree`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        self.dir.truncate().await;
        self.dir
            .create_empty_file(ROOT.to_string(), Node::Leaf(vec![]))
            .await?;

        debug_assert!(
            self.dir.contains(&ROOT),
            "B+Tree failed to re-create its root node"
        );
        Ok(())
    }
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
{
    /// Delete the keys in the `other` B+Tree from this one.
    ///
    /// The source B+Tree **must** have an identical schema and collation.
    pub async fn delete_all<G>(&mut self, other: BTree<S, C, G>) -> Result<(), io::Error>
    where
        G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        validate_collator_eq(&self.collator, &other.collator)?;
        validate_schema_eq(&self.schema, &other.schema)?;

        let mut keys = other.keys(Range::<S::Value>::default()).await?;
        while let Some(key) = keys.try_next().await? {
            self.delete(&key).await?;
        }

        Ok(())
    }
}
