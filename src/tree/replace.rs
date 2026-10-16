//! Order-preserving replacement validates all changes before publication.

use std::io;

use collate::Collate;
use freqfs::{DirWriteGuardOwned, FileLoad};
use safecast::AsType;
use uuid::Uuid;

use super::{BTree, Node, ROOT, Schema, validate_key};
use crate::node::Block;

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Replace an exact row without changing its position or the tree topology.
    /// The replacement must remain strictly between the actual predecessor and
    /// successor, including across leaf boundaries. It need not compare equal to
    /// the previous row. Returns false when the previous row is absent.
    /// Collator-equivalent ambiguity, collisions, and relocation are rejected
    /// before any row contents change.
    /// All validation and guard acquisition precede the synchronous publication;
    /// this does not provide durability or atomicity across multiple indexes.
    pub async fn replace(
        &mut self,
        previous: &[S::Value],
        replacement: Vec<S::Value>,
    ) -> io::Result<bool> {
        if previous.len() != self.schema.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid previous row width",
            ));
        }

        let replacement = validate_key(&*self.schema, replacement)?;

        // Start just before the first equal separator: equal leaf rows may span
        // multiple children. Bounds are copies, never replacement candidates.
        let mut path = Vec::new();
        let mut id = ROOT;
        let node = loop {
            let node = self.dir.read_file(&id).await?;
            match &*node {
                Node::Index(bounds, children) => {
                    if bounds.is_empty() || bounds.len() != children.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid index node",
                        ));
                    }

                    let slot = bounds
                        .bisect_left(previous, &self.collator)
                        .saturating_sub(1);
                    path.push((id, slot));
                    id = children[slot];
                }
                Node::Leaf(rows) => {
                    let slot = rows.bisect_left(previous, &self.collator);
                    path.push((id, slot));

                    if slot == rows.len() {
                        drop(node);
                        path.pop();

                        if !self.adjacent_slot(&mut path, true).await? {
                            return Ok(false);
                        }

                        let &(id, _) = path.last().expect("leaf path");
                        break self.dir.read_file(&id).await?;
                    }

                    break node;
                }
            }
        };

        let mut ordered = true;
        let mut check_neighbor = |row: &[S::Value], forward: bool| {
            if self.collator.cmp_slices(row, previous).is_eq() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ambiguous collator-equivalent rows",
                ));
            }

            let cmp = self.collator.cmp_slices(&replacement, row);
            ordered &= if forward { cmp.is_lt() } else { cmp.is_gt() };
            Ok(())
        };

        let &(_, slot) = path.last().expect("leaf path");
        let mut edges = [false; 2];
        let exact = {
            let Node::Leaf(rows) = &*node else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected replacement leaf",
                ));
            };

            if !self.collator.cmp_slices(&rows[slot], previous).is_eq() {
                return Ok(false);
            }

            for forward in [false, true] {
                let adjacent = if forward {
                    slot.checked_add(1)
                } else {
                    slot.checked_sub(1)
                };
                if let Some(row) = adjacent.and_then(|i| rows.get(i)) {
                    check_neighbor(row, forward)?;
                } else {
                    edges[usize::from(forward)] = true;
                }
            }

            rows[slot] == previous
        };
        drop(node);

        // Only leaf boundaries require another traversal. Release this leaf
        // before reading a neighbor so lookup never pins two payloads.
        for forward in [false, true]
            .into_iter()
            .filter(|forward| edges[usize::from(*forward)])
        {
            let mut neighbor = path[..path.len() - 1].to_vec();
            if self.adjacent_slot(&mut neighbor, forward).await? {
                let &(id, slot) = neighbor.last().expect("neighbor path");
                let node = self.dir.read_file(&id).await?;
                let Node::Leaf(rows) = &*node else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected neighboring leaf",
                    ));
                };
                check_neighbor(&rows[slot], forward)?;
            }
        }

        if !exact {
            return Ok(false);
        }

        if !ordered {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "replacement must remain strictly between its neighbors",
            ));
        }

        if replacement == previous {
            return Ok(true);
        }

        // Only a changed first row propagates upward. Include the first nonzero
        // parent slot, whose own first row remains unchanged.
        let mut first = path.len() - 1;
        while first > 0 && path[first].1 == 0 {
            first -= 1;
        }

        let affected = &path[first..];
        let copies = std::iter::repeat_n(replacement, affected.len());

        let mut prepared = Vec::with_capacity(affected.len());
        for (&(id, slot), row) in affected.iter().zip(copies) {
            let node = self.write_node(&id).await?;
            let rows = match &*node {
                Node::Leaf(rows) | Node::Index(rows, _) => rows,
            };
            rows.get(slot)
                .filter(|row| row.as_slice() == previous)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "replacement bound is inconsistent with its leaf",
                    )
                })?;

            prepared.push((node, slot, row));
        }

        // No await or fallible work after the first swap. The directory write
        // owner excludes readers until every affected first-row copy is updated.
        for (mut node, slot, row) in prepared {
            let rows = match &mut *node {
                Node::Leaf(rows) | Node::Index(rows, _) => rows,
            };
            rows[slot] = row;
        }

        Ok(true)
    }

    // Find an adjacent leaf from the current leaf's ancestors. Guards are
    // released before entering another node or acquiring publication guards.
    async fn adjacent_slot(
        &self,
        path: &mut Vec<(Uuid, usize)>,
        forward: bool,
    ) -> io::Result<bool> {
        while let Some((id, slot)) = path.pop() {
            let node = self.dir.read_file(&id).await?;
            let Node::Index(rows, children) = &*node else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected index ancestor",
                ));
            };

            if rows.is_empty() || rows.len() != children.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid index node",
                ));
            }

            let next = if forward {
                slot.checked_add(1).filter(|i| *i < rows.len())
            } else {
                slot.checked_sub(1)
            };
            let Some(next) = next else { continue };

            path.push((id, next));
            let mut child = children[next];
            drop(node);

            loop {
                let node = self.dir.read_file(&child).await?;
                match &*node {
                    Node::Leaf(rows) => {
                        if rows.is_empty() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "empty non-root leaf",
                            ));
                        }

                        path.push((child, if forward { 0 } else { rows.len() - 1 }));
                        return Ok(true);
                    }
                    Node::Index(rows, children) => {
                        if rows.is_empty() || rows.len() != children.len() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "invalid index node",
                            ));
                        }

                        let slot = if forward { 0 } else { rows.len() - 1 };
                        path.push((child, slot));
                        child = children[slot];
                    }
                }
            }
        }

        Ok(false)
    }
}
