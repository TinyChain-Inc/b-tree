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

/// Immutable range collation and storage ordering for an index.
///
/// Storage ordering preserves every range comparison: the optional storage
/// comparator is applied only after all range columns and key lengths tie.
/// Both value comparators must be consistent and preserve semantic equality.
/// Keep the configuration unchanged when reopening an existing index.
///
/// ```
/// use std::cmp::Ordering;
/// use b_tree::IndexOrder;
/// use collate::{Collate, Collator};
///
/// #[derive(Eq, PartialEq)]
/// struct Magnitude;
/// impl Collate for Magnitude {
///     type Value = i32;
///     fn cmp(&self, left: &i32, right: &i32) -> Ordering {
///         left.unsigned_abs().cmp(&right.unsigned_abs())
///     }
/// }
/// let ordering = IndexOrder::new(Magnitude)
///     .with_storage_order(Collator::<i32>::default());
/// assert!(ordering.range_collator().cmp(&vec![-3], &vec![3]).is_eq());
/// assert!(ordering.key_collator().cmp(&[-3], &[3]).is_lt());
/// ```
///
/// Select a comparison role explicitly; the configuration is not a comparator:
///
/// ```compile_fail
/// use b_tree::IndexOrder;
/// use collate::{Collate, Collator};
/// let ordering = IndexOrder::new(Collator::<u32>::default());
/// Collate::cmp(&ordering, &vec![1u32], &vec![2u32]);
/// ```
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct IndexOrder<C, O = C> {
    range: RangeCollator<C>,
    storage: Option<O>,
}

impl<C: Collate> IndexOrder<C> {
    /// Use range collation alone to order complete keys, including key lengths.
    /// Construction moves the comparator without cloning it.
    pub fn new(range_values: C) -> Self {
        Self {
            range: RangeCollator::new(range_values),
            storage: None,
        }
    }
}

impl<C, O> IndexOrder<C, O> {
    /// Add storage ordering within groups of complete range-equivalent keys.
    /// It cannot reorder different range groups or override later range columns.
    pub fn with_storage_order<S>(self, storage_values: S) -> IndexOrder<C, S>
    where
        C: Collate,
        S: Collate<Value = C::Value>,
    {
        IndexOrder {
            range: self.range,
            storage: Some(storage_values),
        }
    }

    /// Borrow the comparator for ranges, prefixes, and range reservations.
    pub fn range_collator(&self) -> &RangeCollator<C> {
        &self.range
    }

    /// Borrow the comparator for complete stored keys.
    pub fn key_collator(&self) -> KeyCollator<'_, C, O> {
        KeyCollator { ordering: self }
    }
}

/// Comparison of ranges and shared key prefixes using value collation.
/// A matching prefix compares equal regardless of an omitted suffix.
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
    // An omitted suffix sorts before a full row only during routing.
    fn cmp_routing<L, R>(&self, left: &[L], right: &[R]) -> Ordering
    where
        L: Borrow<C::Value>,
        R: Borrow<C::Value>,
    {
        self.cmp_slices(left, right)
            .then_with(|| left.len().cmp(&right.len()))
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

/// A borrowed comparator for complete keys in an index's storage order.
/// Comparisons retain no key values and require no intrinsic `Ord` or value clones.
#[derive(Eq, PartialEq)]
pub struct KeyCollator<'a, C, O = C> {
    ordering: &'a IndexOrder<C, O>,
}

impl<C, O> KeyCollator<'_, C, O>
where
    C: Collate,
    O: Collate<Value = C::Value>,
{
    /// Compare complete borrowed keys, including their lengths.
    pub fn cmp<L, R>(&self, left: &[L], right: &[R]) -> Ordering
    where
        L: Borrow<C::Value>,
        R: Borrow<C::Value>,
    {
        self.cmp_iter(
            left.iter().map(Borrow::borrow),
            right.iter().map(Borrow::borrow),
        )
    }

    /// Compare complete keys exposed as replayable borrowed column iterators.
    /// Only the iterators are cloned; no projected rows are allocated.
    pub fn cmp_iter<'a, 'b, L, R>(&self, left: L, right: R) -> Ordering
    where
        C::Value: 'a + 'b,
        L: ExactSizeIterator<Item = &'a C::Value> + Clone,
        R: ExactSizeIterator<Item = &'b C::Value> + Clone,
    {
        cmp_values(&self.ordering.range.value, left.clone(), right.clone())
            .then_with(|| left.len().cmp(&right.len()))
            .then_with(|| match &self.ordering.storage {
                Some(storage) => cmp_values(storage, left, right),
                None => Ordering::Equal,
            })
    }
}

impl<C, O> Collate for KeyCollator<'_, C, O>
where
    C: Collate,
    O: Collate<Value = C::Value>,
{
    type Value = Vec<C::Value>;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        self.cmp(left.as_slice(), right.as_slice())
    }
}

impl<C, O> CollateRef<[C::Value]> for KeyCollator<'_, C, O>
where
    C: Collate,
    O: Collate<Value = C::Value>,
{
    fn cmp_ref(&self, left: &[C::Value], right: &[C::Value]) -> Ordering {
        self.cmp(left, right)
    }
}

impl<C, O> CollateRef<Key<C::Value>> for KeyCollator<'_, C, O>
where
    C: Collate,
    O: Collate<Value = C::Value>,
{
    fn cmp_ref(&self, left: &Key<C::Value>, right: &Key<C::Value>) -> Ordering {
        self.cmp(left.as_slice(), right.as_slice())
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
