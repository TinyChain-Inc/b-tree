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

/// A collator used by a B+Tree
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

/// The schema of a B+Tree
pub trait Schema: Eq + fmt::Debug {
    /// The type of error returned by [`Schema::validate_key`]
    type Error: std::error::Error + Send + Sync + 'static;

    /// The type of value stored in a B+Tree's keys
    type Value: Default + Clone + Eq + Send + Sync + fmt::Debug + 'static;

    /// Get the maximum size in bytes of a leaf node in a B+Tree with this [`Schema`].
    fn block_size(&self) -> usize;

    /// Get the number of columns in this [`Schema`].
    fn len(&self) -> usize;

    /// Get the order of the nodes in a B+Tree with this [`Schema`].
    fn order(&self) -> usize;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;
}
