//! Order-preserving replacement validates all changes before publication.

use std::io;

use collate::Collate;
use freqfs::{DirWriteGuardOwned, FileLoad};
use safecast::AsType;

use super::{BTree, Node, Schema, validate_children, validate_key};

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
    /// The supplied row is published even when semantically equal to the old row.
    /// Only routing separators crossed by the replacement are updated; no
    /// delete/reinsert or additional caller-side ordering guard is needed.
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

        if rows.get(slot).is_none_or(|row| row.as_slice() != previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "replacement target changed",
            ));
        }

        let local = [
            slot.checked_sub(1),
            slot.checked_add(1).filter(|&i| i < rows.len()),
        ]
        .map(|next| next.map(|i| self.collator.cmp_slices(&replacement, &rows[i])));
        drop(node);

        // Only a changed leaf extreme can cross a routing separator. At most two
        // ancestor separators need preparation, regardless of tree height.
        let mut boundaries = Vec::with_capacity(2);
        for (forward, local) in [false, true].into_iter().zip(local) {
            let order = if let Some(order) = local {
                order
            } else {
                let mut neighbor_path = path.clone();
                let Some(neighbor) = self.adjacent_leaf(&mut neighbor_path, forward).await? else {
                    continue;
                };
                let Node::Leaf(rows) = &*neighbor else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "expected leaf"));
                };

                let neighbor_slot = neighbor_path.last().expect("neighbor path").1;
                let neighbor_row = &rows[neighbor_slot];
                let order = self.collator.cmp_slices(&replacement, neighbor_row);

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

                let level = path
                    .iter()
                    .zip(&neighbor_path)
                    .position(|(target, neighbor)| target.1 != neighbor.1)
                    .expect("adjacent subtree divergence");
                let boundary = path[level].1.min(neighbor_path[level].1);
                let ancestor = self.dir.read_file(&path[level].0).await?;
                let Node::Index(separators, children) = &*ancestor else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "expected index ancestor",
                    ));
                };

                validate_children(separators, children)?;
                let separator = separators.get(boundary).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing replacement separator")
                })?;
                let previous_order = self.collator.cmp_routing(previous, separator);
                let neighbor_order = self.collator.cmp_routing(neighbor_row, separator);

                if if forward {
                    previous_order.is_gt() || neighbor_order.is_lt()
                } else {
                    previous_order.is_lt() || neighbor_order.is_gt()
                } {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid replacement separator",
                    ));
                }

                let crosses = self.collator.cmp_routing(&replacement, separator);

                if if forward {
                    crosses.is_gt()
                } else {
                    crosses.is_lt()
                } {
                    let updated = if forward {
                        self.collator.separator(&replacement, neighbor_row)
                    } else {
                        self.collator.separator(neighbor_row, &replacement)
                    };
                    boundaries.push((level, boundary, separator.clone(), updated));
                }

                order
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

        // Acquire affected ancestors once, root-to-leaf, then the leaf. All
        // separator validation and allocation precede synchronous publication.
        boundaries.sort_unstable_by_key(|&(level, _, _, _)| level);
        let mut boundaries = boundaries.into_iter().peekable();
        let mut prepared = Vec::with_capacity(2);

        while let Some(&(level, _, _, _)) = boundaries.peek() {
            let node = self.write_node(&path[level].0).await?;
            let Node::Index(separators, children) = &*node else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected index ancestor",
                ));
            };

            validate_children(separators, children)?;
            let mut changes = Vec::with_capacity(2);
            while boundaries
                .peek()
                .is_some_and(|&(next, _, _, _)| next == level)
            {
                let (_, slot, previous, replacement) =
                    boundaries.next().expect("prepared separator");
                if separators.get(slot) != Some(&previous) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "replacement separator changed",
                    ));
                }
                changes.push((slot, replacement));
            }

            prepared.push((node, changes));
        }

        let mut leaf = self.write_node(&id).await?;
        let Node::Leaf(rows) = &mut *leaf else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected leaf"));
        };

        if rows.get(slot).is_none_or(|row| row.as_slice() != previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "replacement target changed",
            ));
        }

        // No await or fallible work after the first swap. The directory write
        // owner excludes readers until all affected routing separators are updated.
        for (mut node, changes) in prepared {
            let Node::Index(separators, _) = &mut *node else {
                unreachable!("validated index ancestor")
            };

            for (slot, replacement) in changes {
                separators[slot] = replacement;
            }
        }

        rows[slot] = replacement;

        Ok(true)
    }
}
