//! Deletion owns borrowing, merging, and root collapse.

use std::borrow::Borrow;
use std::{io, mem, pin::Pin};

use collate::Collate;
use freqfs::{DirDeref, DirWriteGuardOwned, FileLoad, FileWriteGuardOwned};
use futures::TryStreamExt;
use futures::future::Future;
use get_size::GetSize;
use safecast::AsType;
use uuid::Uuid;

use super::{BTree, Node, ROOT, Schema, validate_collator_eq, validate_schema_eq};
use crate::node::Block;
use crate::range::Range;

type ResultDelete<FE, V> = Result<Delete<FE, V>, io::Error>;
type ResultMergeIndex<V> = Result<MergeIndexLeft<V>, io::Error>;
type ResultMergeLeaf<V> = Result<MergeLeafLeft<V>, io::Error>;

enum MergeIndexLeft<V> {
    Borrow(Vec<V>),
    Merge(Vec<V>),
}

enum MergeIndexRight {
    Borrow,
    Merge,
}

enum MergeLeafLeft<V> {
    Borrow(Vec<V>),
    Merge(Vec<V>),
}

enum MergeLeafRight {
    Borrow,
    Merge,
}

enum Delete<FE: GetSize, V> {
    None,
    Left(Vec<V>),
    Right,
    Underflow(FileWriteGuardOwned<FE, Node<V>>),
}

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
        let mut root = self.write_node(&ROOT).await?;

        let new_root = match &mut *root {
            Node::Leaf(keys) => {
                let i = keys.bisect_left(key, &self.collator);
                if i < keys.len() && keys[i].iter().zip(key).all(|(l, r)| l == r.borrow()) {
                    keys.remove(i);
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }
            Node::Index(bounds, children) => {
                let i = match bounds.bisect_right(key, &self.collator) {
                    0 => return Ok(false),
                    i => i - 1,
                };

                let node = self.write_node(&children[i]).await?;
                match self.delete_inner(node, key).await? {
                    Delete::None => return Ok(false),
                    Delete::Right => return Ok(true),
                    Delete::Left(bound) => {
                        let Node::Index(bounds, _) = &mut *root else {
                            unreachable!("index")
                        };
                        bounds[i] = bound;
                        return Ok(true);
                    }
                    Delete::Underflow(mut node) => {
                        let Node::Index(bounds, children) = &mut *root else {
                            unreachable!("index")
                        };
                        let retired = match &mut *node {
                            Node::Leaf(new_keys) => {
                                self.merge_leaf(new_keys, i, bounds, children).await?
                            }
                            Node::Index(new_bounds, new_children) => {
                                self.merge_index(new_bounds, new_children, i, bounds, children)
                                    .await?
                            }
                        };
                        drop(node);
                        if let Some(id) = retired {
                            self.dir.delete(&id).await;
                        }
                    }
                }
                let Node::Index(bounds, children) = &mut *root else {
                    unreachable!("index")
                };

                if children.len() == 1 {
                    bounds.pop();
                    children.pop()
                } else {
                    None
                }
            }
        };

        if let Some(only_child) = new_root {
            let new_root = {
                let mut child = self.write_node(&only_child).await?;
                match &mut *child {
                    Node::Leaf(keys) => Node::Leaf(mem::take(keys)),
                    Node::Index(bounds, children) => {
                        Node::Index(mem::take(bounds), mem::take(children))
                    }
                }
            };

            self.dir.delete(&only_child).await;

            *root = new_root;
        }

        Ok(true)
    }

    fn delete_inner<'a, V>(
        &'a mut self,
        mut node: FileWriteGuardOwned<FE, Node<S::Value>>,
        key: &'a [V],
    ) -> Pin<Box<dyn Future<Output = ResultDelete<FE, S::Value>> + Send + 'a>>
    where
        V: Borrow<S::Value> + Send + Sync,
    {
        Box::pin(async move {
            match &mut *node {
                Node::Leaf(keys) => {
                    let i = keys.bisect_left(key, &self.collator);

                    if i < keys.len() && keys[i].iter().zip(key).all(|(l, r)| l == r.borrow()) {
                        keys.remove(i);

                        if keys.len() < (self.schema.order() / 2) {
                            Ok(Delete::Underflow(node))
                        } else if i == 0 {
                            Ok(Delete::Left(keys[0].to_vec()))
                        } else {
                            Ok(Delete::Right)
                        }
                    } else {
                        Ok(Delete::None)
                    }
                }
                Node::Index(bounds, children) => {
                    let i = match bounds.bisect_right(key, &self.collator) {
                        0 => return Ok(Delete::None),
                        i => i - 1,
                    };

                    let child = self.write_node(&children[i]).await?;
                    match self.delete_inner(child, key).await? {
                        Delete::None => return Ok(Delete::None),
                        Delete::Right => return Ok(Delete::Right),
                        Delete::Left(bound) => {
                            let Node::Index(bounds, _) = &mut *node else {
                                unreachable!("index")
                            };
                            bounds[i] = bound;

                            return if i == 0 {
                                Ok(Delete::Left(bounds[0].to_vec()))
                            } else {
                                Ok(Delete::Right)
                            };
                        }
                        Delete::Underflow(mut child) => {
                            let Node::Index(bounds, children) = &mut *node else {
                                unreachable!("index")
                            };
                            let retired = match &mut *child {
                                Node::Leaf(new_keys) => {
                                    self.merge_leaf(new_keys, i, bounds, children).await?
                                }
                                Node::Index(new_bounds, new_children) => {
                                    self.merge_index(new_bounds, new_children, i, bounds, children)
                                        .await?
                                }
                            };
                            drop(child);
                            if let Some(id) = retired {
                                self.dir.delete(&id).await;
                            }
                        }
                    }
                    let Node::Index(bounds, children) = &mut *node else {
                        unreachable!("index")
                    };

                    if children.len() > (self.schema.order() / 2) {
                        if i == 0 {
                            Ok(Delete::Left(bounds[0].to_vec()))
                        } else {
                            Ok(Delete::Right)
                        }
                    } else {
                        Ok(Delete::Underflow(node))
                    }
                }
            }
        })
    }

    fn merge_index<'a>(
        &'a mut self,
        new_bounds: &'a mut Vec<Vec<S::Value>>,
        new_children: &'a mut Vec<Uuid>,
        i: usize,
        bounds: &'a mut Vec<Vec<S::Value>>,
        children: &'a mut Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Uuid>, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut retired = None;
            if i == 0 {
                match self
                    .merge_index_left(new_bounds, new_children, &children[i + 1])
                    .await?
                {
                    MergeIndexLeft::Borrow(bound) => {
                        bounds[i] = new_bounds[0].to_vec();
                        bounds[i + 1] = bound;
                    }
                    MergeIndexLeft::Merge(bound) => {
                        retired = Some(children[0]);
                        children.remove(0);
                        bounds.remove(0);
                        bounds[0] = bound;
                    }
                }
            } else {
                match self
                    .merge_index_right(new_bounds, new_children, &children[i - 1])
                    .await?
                {
                    MergeIndexRight::Borrow => {
                        bounds[i] = new_bounds[0].to_vec();
                    }
                    MergeIndexRight::Merge => {
                        retired = Some(children[i]);
                        children.remove(i);
                        bounds.remove(i);
                    }
                }
            }

            Ok(retired)
        })
    }

    fn merge_index_left<'a>(
        &'a self,
        left_bounds: &'a mut Vec<Vec<S::Value>>,
        left_children: &'a mut Vec<Uuid>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = ResultMergeIndex<S::Value>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.write_node(node_id).await?;
            match &mut *node {
                Node::Leaf(_right_keys) => unreachable!("merge a leaf node with an index node"),
                Node::Index(right_bounds, right_children) => {
                    if right_bounds.len() > (self.schema.order() / 2) {
                        left_bounds.push(right_bounds.remove(0));
                        left_children.push(right_children.remove(0));
                        Ok(MergeIndexLeft::Borrow(right_bounds[0].to_vec()))
                    } else {
                        let mut new_bounds =
                            Vec::with_capacity(left_bounds.len() + right_bounds.len());

                        new_bounds.append(left_bounds);
                        new_bounds.append(right_bounds);
                        *right_bounds = new_bounds;

                        let mut new_children = Vec::with_capacity(right_bounds.len());

                        new_children.append(left_children);
                        new_children.append(right_children);
                        *right_children = new_children;

                        Ok(MergeIndexLeft::Merge(right_bounds[0].to_vec()))
                    }
                }
            }
        })
    }

    fn merge_index_right<'a>(
        &'a self,
        right_bounds: &'a mut Vec<Vec<S::Value>>,
        right_children: &'a mut Vec<Uuid>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<MergeIndexRight, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.write_node(node_id).await?;
            match &mut *node {
                Node::Leaf(_left_keys) => unreachable!("merge a leaf node with an index node"),
                Node::Index(left_bounds, left_children) => {
                    if left_children.len() > (self.schema.order() / 2) {
                        let right = left_bounds.pop().expect("right");
                        right_bounds.insert(0, right);

                        let right = left_children.pop().expect("right");
                        right_children.insert(0, right);

                        Ok(MergeIndexRight::Borrow)
                    } else {
                        left_bounds.append(right_bounds);
                        left_children.append(right_children);
                        Ok(MergeIndexRight::Merge)
                    }
                }
            }
        })
    }

    fn merge_leaf<'a>(
        &'a mut self,
        new_keys: &'a mut Vec<Vec<S::Value>>,
        i: usize,
        bounds: &'a mut Vec<Vec<S::Value>>,
        children: &'a mut Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Uuid>, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut retired = None;
            if i == 0 {
                match self.merge_leaf_left(new_keys, &children[i + 1]).await? {
                    MergeLeafLeft::Borrow(bound) => {
                        bounds[i] = new_keys[0].to_vec();
                        bounds[i + 1] = bound;
                    }
                    MergeLeafLeft::Merge(bound) => {
                        retired = Some(children[0]);
                        children.remove(0);
                        bounds.remove(0);
                        bounds[0] = bound;
                    }
                }
            } else {
                match self.merge_leaf_right(new_keys, &children[i - 1]).await? {
                    MergeLeafRight::Borrow => {
                        bounds[i] = new_keys[0].to_vec();
                    }
                    MergeLeafRight::Merge => {
                        retired = Some(children[i]);
                        children.remove(i);
                        bounds.remove(i);
                    }
                }
            }

            Ok(retired)
        })
    }

    fn merge_leaf_left<'a>(
        &'a self,
        left_keys: &'a mut Vec<Vec<S::Value>>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = ResultMergeLeaf<S::Value>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.write_node(node_id).await?;
            match &mut *node {
                Node::Leaf(right_keys) => {
                    if right_keys.len() > (self.schema.order() / 2) {
                        left_keys.push(right_keys.remove(0));
                        Ok(MergeLeafLeft::Borrow(right_keys[0].to_vec()))
                    } else {
                        let mut new_keys = Vec::with_capacity(left_keys.len() + right_keys.len());
                        new_keys.append(left_keys);
                        new_keys.append(right_keys);
                        *right_keys = new_keys;

                        Ok(MergeLeafLeft::Merge(right_keys[0].to_vec()))
                    }
                }
                Node::Index(_, children) => {
                    let result = self.merge_leaf_left(left_keys, &children[0]).await?;
                    let Node::Index(bounds, _) = &mut *node else {
                        unreachable!("index")
                    };
                    match result {
                        MergeLeafLeft::Borrow(left) => {
                            bounds[0] = left.to_vec();
                            Ok(MergeLeafLeft::Borrow(left))
                        }
                        MergeLeafLeft::Merge(left) => {
                            bounds[0] = left.to_vec();
                            Ok(MergeLeafLeft::Merge(left))
                        }
                    }
                }
            }
        })
    }

    fn merge_leaf_right<'a>(
        &'a self,
        right_keys: &'a mut Vec<Vec<S::Value>>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<MergeLeafRight, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.write_node(node_id).await?;
            match &mut *node {
                Node::Leaf(left_keys) => {
                    if left_keys.len() > (self.schema.order() / 2) {
                        let right = left_keys.pop().expect("right");
                        right_keys.insert(0, right);
                        Ok(MergeLeafRight::Borrow)
                    } else {
                        left_keys.append(right_keys);
                        Ok(MergeLeafRight::Merge)
                    }
                }
                Node::Index(_bounds, _children) => unreachable!("merge with the rightmost leaf"),
            }
        })
    }

    /// Insert the given `key` into this B+Tree.
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
