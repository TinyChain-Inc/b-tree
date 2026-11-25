//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;

use collate::{Collate, CollateRef};

mod node;
mod range;
mod tree;

pub use collate;
pub use node::{Block, Node};
pub use range::Range;
pub use tree::{BTree, BTreeLock, BTreeReadGuard, BTreeWriteGuard, Keys};

const NODE_STACK_SIZE: usize = 32;

/// The size limit for a [`Key`] in a stream to be stack-allocated
pub const KEY_STACK_SIZE: usize = 32;

/// The type of item in a stream of B+Tree keys
pub type Key<V> = smallvec::SmallVec<[V; KEY_STACK_SIZE]>;

/// Comparison of ranges and shared key prefixes using value collation.
/// A matching prefix compares equal regardless of an omitted suffix.
/// Use [`Self::cmp_keys`] or [`Self::cmp_key_iter`] for complete keys instead.
/// Keep the value comparator unchanged when reopening an index.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct RangeCollator<C> {
    value: C,
}

impl<C> RangeCollator<C> {
    /// Construct range comparison from an ordinary value comparator.
    pub fn new(value: C) -> Self {
        Self { value }
    }

    /// Borrow the value comparator used by range and prefix comparison.
    pub fn inner(&self) -> &C {
        &self.value
    }
}

impl<C: Collate> RangeCollator<C> {
    /// Compare complete borrowed keys lexicographically, including their lengths.
    pub fn cmp_keys<L, R>(&self, left: &[L], right: &[R]) -> Ordering
    where
        L: Borrow<C::Value>,
        R: Borrow<C::Value>,
    {
        self.cmp_key_iter(
            left.iter().map(Borrow::borrow),
            right.iter().map(Borrow::borrow),
        )
    }

    /// Compare complete keys in one pass without allocating or cloning values or iterators.
    pub fn cmp_key_iter<'a, 'b, L, R>(&self, left: L, right: R) -> Ordering
    where
        C::Value: 'a + 'b,
        L: ExactSizeIterator<Item = &'a C::Value>,
        R: ExactSizeIterator<Item = &'b C::Value>,
    {
        let lengths = left.len().cmp(&right.len());
        cmp_values(&self.value, left, right).then(lengths)
    }

    // Retain the full right row when all range columns collate equally.
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

    fn cmp_slices<L, R>(&self, left: &[L], right: &[R]) -> Ordering
    where
        L: Borrow<C::Value>,
        R: Borrow<C::Value>,
    {
        cmp_values(
            &self.value,
            left.iter().map(Borrow::borrow),
            right.iter().map(Borrow::borrow),
        )
    }
}

impl<C: Collate> Collate for RangeCollator<C> {
    type Value = Vec<C::Value>;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        self.cmp_slices(left, right)
    }
}

impl<C: Collate> CollateRef<[C::Value]> for RangeCollator<C> {
    fn cmp_ref(&self, left: &[C::Value], right: &[C::Value]) -> Ordering {
        self.cmp_slices(left, right)
    }
}

impl<C: Collate> CollateRef<Key<C::Value>> for RangeCollator<C> {
    fn cmp_ref(&self, left: &Key<C::Value>, right: &Key<C::Value>) -> Ordering {
        self.cmp_slices(left, right)
    }
}

fn cmp_values<'a, 'b, C, L, R>(collator: &C, left: L, right: R) -> Ordering
where
    C: Collate,
    C::Value: 'a + 'b,
    L: Iterator<Item = &'a C::Value>,
    R: Iterator<Item = &'b C::Value>,
{
    left.zip(right)
        .map(|(left, right)| collator.cmp(left, right))
        .find(|order| !order.is_eq())
        .unwrap_or(Ordering::Equal)
}

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
