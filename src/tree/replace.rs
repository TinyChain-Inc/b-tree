//! Order-preserving replacement validates all changes before publication.

use std::io;

use collate::Collate;
use freqfs::{DirWriteGuardOwned, FileLoad};
use safecast::AsType;

use super::{BTree, Node, Schema, validate_key};

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Replace an exact row without changing its position or the tree topology.
    ///
    /// The replacement must remain between the actual predecessor and
    /// successor, including across leaf boundaries. It need not compare equal to
    /// the previous row. Returns false when the previous row is absent.
    ///
    /// Distinct collator-equivalent rows are allowed. Value collisions and
    /// relocation are rejected before any row contents change.
    ///
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

        // Search the replacement group for both identities. This avoids comparing
        // two potentially large rows merely to decide whether they share a group.
        let mut target = None;
        let replacement_check = self
            .visit_equivalent(&replacement, |path, row| {
                if row == previous {
                    if target.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "duplicate semantic row",
                        ));
                    }

                    target = Some(path.to_vec());
                } else if row == replacement {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "replacement collides with an existing value",
                    ));
                }

                Ok(false)
            })
            .await;

        // Distinct groups require a second search. Resolve the previous row before
        // reporting replacement errors, preserving absent and ambiguous-target behavior.
        if target.is_none() || replacement_check.is_err() {
            target = None;
            self.visit_equivalent(previous, |path, row| {
                if row == previous {
                    if target.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "duplicate semantic row",
                        ));
                    }

                    target = Some(path.to_vec());
                }

                Ok(false)
            })
            .await?;
        }

        let Some(path) = target else { return Ok(false) };
        replacement_check?;

        let &(id, slot) = path.last().expect("leaf path");
        let node = self.dir.read_file(&id).await?;
        let Node::Leaf(rows) = &*node else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected leaf"));
        };

        let local = [
            slot.checked_sub(1),
            slot.checked_add(1).filter(|&i| i < rows.len()),
        ]
        .map(|next| next.map(|i| self.collator.cmp_slices(&replacement, &rows[i])));
        drop(node);

        for (forward, order) in [false, true].into_iter().zip(local) {
            let order = match order {
                Some(order) => order,
                None => {
                    let mut neighbor = path.clone();
                    let Some(node) = self.adjacent_leaf(&mut neighbor, forward).await? else {
                        continue;
                    };
                    let Node::Leaf(rows) = &*node else {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected leaf"));
                    };

                    let slot = neighbor.last().expect("neighbor path").1;
                    self.collator.cmp_slices(&replacement, &rows[slot])
                }
            };

            if if forward {
                order.is_gt()
            } else {
                order.is_lt()
            } {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replacement must remain between its neighbors",
                ));
            }
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
}
