//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;

use collate::{Collate, CollateRef};

mod group;
mod node;
mod range;
mod tree;

pub use node::{Block, Node};
pub use range::Range;
pub use tree::{BTree, BTreeLock, BTreeReadGuard, BTreeWriteGuard, Keys};

pub use collate;

const NODE_STACK_SIZE: usize = 32;

/// The size limit for a [`Key`] in a stream to be stack-allocated
pub const KEY_STACK_SIZE: usize = 32;

/// The type of item in a stream of B+Tree keys
pub type Key<V> = smallvec::SmallVec<[V; KEY_STACK_SIZE]>;

/// Compares tree keys using a caller-supplied value collator.
///
/// Public key comparison compares shared columns: `[7]` matches the prefix of
/// `[7, 42]`. Private separator routing instead orders `[7]` before `[7, 42]`,
/// treating an omitted suffix as negative infinity.
///
/// B-tree owns separator construction and routing; callers only supply value
/// comparison. Collation equality does not establish semantic identity: exact
/// row matching checks value equality separately.
#[derive(Copy, Clone)]
pub struct Collator<C> {
    value: C,
}

impl<C> Collator<C> {
    /// Construct a new [`Collator`] for a B+Tree from an existing `value` collator.
    pub fn new(value: C) -> Self {
        Self { value }
    }

    /// Borrow the value collator used by this key [`Collator`].
    pub fn inner(&self) -> &C {
        &self.value
    }
}

impl<C: Collate> Collator<C> {
    // Compare shared columns, then lengths, treating an omitted suffix as
    // negative infinity. Full rows therefore sort after their proper prefixes.
    fn cmp_routing<L, R>(&self, left: &[L], right: &[R]) -> Ordering
    where
        L: Borrow<C::Value>,
        R: Borrow<C::Value>,
    {
        self.cmp_slices(left, right)
            .then_with(|| left.len().cmp(&right.len()))
    }

    // Keep the right prefix through the first differing column. When all columns
    // collate equally, retain the full row: truncation would put the separator
    // before both neighboring rows instead of between them.
    fn separator(&self, left: &[C::Value], right: &[C::Value]) -> Vec<C::Value>
    where
        C::Value: Clone,
    {
        let width = left
            .iter()
            .zip(right)
            .position(|(left, right)| !self.value.cmp(left, right).is_eq())
            .map_or(right.len(), |i| i + 1);

        right[..width].to_vec()
    }

    // Compare only shared columns; matching prefixes compare equal.
    fn cmp_slices<L, R>(&self, left: &[L], right: &[R]) -> Ordering
    where
        L: Borrow<C::Value>,
        R: Borrow<C::Value>,
    {
        for i in 0..Ord::min(left.len(), right.len()) {
            match self.value.cmp(left[i].borrow(), right[i].borrow()) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }

        Ordering::Equal
    }
}

impl<C: Collate> Collate for Collator<C> {
    type Value = Vec<C::Value>;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        self.cmp_slices(left, right)
    }
}

impl<C: Collate> CollateRef<[C::Value]> for Collator<C> {
    fn cmp_ref(&self, left: &[C::Value], right: &[C::Value]) -> Ordering {
        self.cmp_slices(left, right)
    }
}

impl<C: Collate> CollateRef<Key<C::Value>> for Collator<C> {
    fn cmp_ref(&self, left: &Key<C::Value>, right: &Key<C::Value>) -> Ordering {
        self.cmp_slices(left, right)
    }
}

impl<C> PartialEq for Collator<C>
where
    C: Collate,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<C> Eq for Collator<C> where C: Collate {}

/// The schema of a B+Tree.
///
/// A schema always has at least one key column, so `len` does not imply a
/// meaningful `is_empty` operation.
#[allow(clippy::len_without_is_empty)]
pub trait Schema: Eq + fmt::Debug {
    /// The type of error returned by [`Schema::validate_key`]
    type Error: std::error::Error + Send + Sync + 'static;

    /// The type of value stored in a B+Tree's keys
    type Value: Default + Clone + Eq + get_size::GetSize + Send + Sync + fmt::Debug + 'static;

    /// Estimate leaf bytes for storage allocation; `order` bounds key count.
    /// Actual encoded and retained sizes are accounted for by the file adapter.
    fn block_size(&self) -> usize;

    /// Get the number of values in each key.
    fn len(&self) -> usize;

    /// Get the order of the nodes in a B+Tree with this [`Schema`].
    /// Must be at least three so splitting preserves two children per index.
    fn order(&self) -> usize;

    /// Validate a nonempty routing prefix, including every supplied column type.
    /// A prefix must contain no more than [`Schema::len`] columns.
    fn validate_prefix(&self, prefix: &[Self::Value]) -> Result<(), Self::Error>;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;
}
