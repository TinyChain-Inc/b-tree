use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::ops::{Bound, Range as Bounds};

use collate::{Collate, Overlap, OverlapsRange, OverlapsValue};
use smallvec::smallvec;

use super::{Collator, Key};

/// A range used to select a slice of a `BTree`
#[derive(Clone, Eq, PartialEq)]
pub struct Range<V> {
    prefix: Key<V>,
    start: Bound<V>,
    end: Bound<V>,
}

impl<V> Default for Range<V> {
    fn default() -> Self {
        Self {
            prefix: smallvec![],
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl<V> Range<V> {
    /// Construct a new [`Range`] with the given `prefix`.
    pub fn with_bounds<K: Into<Key<V>>>(prefix: K, bounds: (Bound<V>, Bound<V>)) -> Self {
        let prefix = prefix.into();
        let (start, end) = bounds;
        Self { prefix, start, end }
    }

    /// Construct a new [`Range`] with the given `prefix`.
    pub fn with_range<K: Into<Key<V>>>(prefix: K, range: Bounds<V>) -> Self {
        let Bounds { start, end } = range;
        Self::with_bounds(prefix, (Bound::Included(start), Bound::Excluded(end)))
    }

    /// Construct a new [`Range`] with only the given `prefix`.
    pub fn from_prefix<K: Into<Key<V>>>(prefix: K) -> Self {
        Self {
            prefix: prefix.into(),
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Construct an owned [`Range`] by borrowing this [`Range`].
    pub fn as_ref(&self) -> Range<&V> {
        Range {
            prefix: self.prefix.iter().collect(),
            start: self.start.as_ref(),
            end: self.end.as_ref(),
        }
    }

    /// Destructure this [`Range`] into a prefix and [`Bound`]s.
    pub fn into_inner(self) -> (Key<V>, (Bound<V>, Bound<V>)) {
        (self.prefix, (self.start, self.end))
    }

    /// Return `false` if this [`Range`] has only a prefix.
    pub fn has_bounds(&self) -> bool {
        !matches!((&self.start, &self.end), (Bound::Unbounded, Bound::Unbounded))
    }

    /// Return `true` if this [`Range`] is empty.
    pub fn is_default(&self) -> bool {
        self.prefix.is_empty() && matches!((&self.start, &self.end), (Bound::Unbounded, Bound::Unbounded))
    }

    /// Return `true` if this [`Range`] is empty.
    pub fn is_empty(&self) -> bool {
        self.is_default()
    }

    /// Return the number of columns specified by this [`Range`].
    pub fn len(&self) -> usize {
        if self.has_bounds() {
            self.prefix.len() + 1
        } else {
            self.prefix.len()
        }
    }

    /// Construct a new [`Range`] by prepending the given `prefix` to this one.
    pub fn prepend<I: IntoIterator<Item = V>>(self, prefix: I) -> Self {
        Self {
            prefix: prefix.into_iter().chain(self.prefix).collect(),
            start: self.start,
            end: self.end,
        }
    }
}

impl<BV, C> OverlapsValue<Vec<C::Value>, Collator<C>> for Range<BV>
where
    BV: Borrow<C::Value>,
    C: Collate,
    C::Value: fmt::Debug,
{
    fn overlaps_value(&self, key: &Vec<C::Value>, collator: &Collator<C>) -> Overlap {
        match collator.cmp_slices(&self.prefix, key) {
            Ordering::Less => Overlap::Less,
            Ordering::Greater => Overlap::Greater,
            Ordering::Equal if self.prefix.len() >= key.len() => Overlap::Narrow,
            Ordering::Equal => {
                let value = &key[self.prefix.len()];

                let start = match &self.start {
                    Bound::Unbounded => Ordering::Less,
                    Bound::Included(start) => collator.value.cmp(start.borrow(), value),
                    Bound::Excluded(start) => match collator.value.cmp(start.borrow(), value) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Greater | Ordering::Equal => Ordering::Greater,
                    },
                };

                let end = match &self.end {
                    Bound::Unbounded => Ordering::Greater,
                    Bound::Included(end) => collator.value.cmp(end.borrow(), value),
                    Bound::Excluded(end) => match collator.value.cmp(end.borrow(), value) {
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Less | Ordering::Equal => Ordering::Less,
                    },
                };

                match (start, end) {
                    (start, Ordering::Less) => {
                        debug_assert!(start != Ordering::Greater);
                        Overlap::Less
                    }
                    (Ordering::Greater, end) => {
                        debug_assert_eq!(end, Ordering::Greater);
                        Overlap::Greater
                    }
                    (Ordering::Equal, Ordering::Equal) if key.len() == self.prefix.len() + 1 => {
                        // in this case, the range prefix of length n is exactly equal to key[0..n]
                        // and the trailing range is exactly equal to key[n]
                        Overlap::Equal
                    }
                    _ => Overlap::Wide,
                }
            }
        }
    }
}

impl<C: Collate> OverlapsRange<Range<C::Value>, Collator<C>> for Range<C::Value> {
    fn overlaps(&self, other: &Range<C::Value>, collator: &Collator<C>) -> Overlap {
        #[inline]
        fn cmp_start<C>(collator: &C, bound: &Bound<C::Value>, value: &C::Value) -> Ordering
        where
            C: Collate,
        {
            match bound {
                Bound::Unbounded => Ordering::Less,
                Bound::Included(start) => collator.cmp(start, value),
                Bound::Excluded(start) => match collator.cmp(start, value) {
                    Ordering::Less | Ordering::Equal => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                },
            }
        }

        #[inline]
        fn cmp_end<C>(collator: &C, bound: &Bound<C::Value>, value: &C::Value) -> Ordering
        where
            C: Collate,
        {
            match bound {
                Bound::Unbounded => Ordering::Less,
                Bound::Included(end) => collator.cmp(end, value),
                Bound::Excluded(end) => match collator.cmp(end, value) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Greater | Ordering::Equal => Ordering::Greater,
                },
            }
        }

        match collator.cmp_slices(&self.prefix, &other.prefix) {
            Ordering::Less => Overlap::Less,
            Ordering::Greater => Overlap::Greater,
            Ordering::Equal => match self.prefix.len().cmp(&other.prefix.len()) {
                Ordering::Less => {
                    let value = &other.prefix[self.prefix.len()];

                    match (
                        cmp_start(&collator.value, &self.start, value),
                        cmp_end(&collator.value, &self.end, value),
                    ) {
                        (Ordering::Greater, _) => Overlap::Greater,
                        (_, Ordering::Less) => Overlap::Less,

                        (Ordering::Equal, Ordering::Greater) => Overlap::WideGreater,
                        (Ordering::Less, Ordering::Equal) => Overlap::WideLess,

                        (Ordering::Less, Ordering::Greater) => Overlap::Wide,
                        (Ordering::Equal, Ordering::Equal) => Overlap::Wide,
                    }
                }
                Ordering::Equal => {
                    (&self.start, &self.end).overlaps(&(&other.start, &other.end), &collator.value)
                }
                Ordering::Greater => {
                    let value = &self.prefix[other.prefix.len()];

                    match (
                        cmp_start(&collator.value, &other.start, value),
                        cmp_end(&collator.value, &other.end, value),
                    ) {
                        (Ordering::Greater, _) => Overlap::Less,
                        (_, Ordering::Less) => Overlap::Greater,
                        _ => Overlap::Narrow,
                    }
                }
            },
        }
    }
}

impl<V> From<Key<V>> for Range<V> {
    fn from(prefix: Key<V>) -> Self {
        Self {
            prefix,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl<V, K: Into<Key<V>>> From<(K, Bounds<V>)> for Range<V> {
    fn from(tuple: (K, Bounds<V>)) -> Self {
        let (prefix, suffix) = tuple;
        let Bounds { start, end } = suffix;

        Self {
            prefix: prefix.into(),
            start: Bound::Included(start),
            end: Bound::Excluded(end),
        }
    }
}

impl<K: fmt::Debug> fmt::Debug for Range<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "range ")?;

        match (&self.start, &self.end) {
            (Bound::Excluded(l), Bound::Unbounded) => write!(f, "[{:?},)", l),
            (Bound::Excluded(l), Bound::Excluded(r)) => write!(f, "[{:?}, {:?}]", l, r),
            (Bound::Excluded(l), Bound::Included(r)) => write!(f, "[{:?}, {:?})", l, r),
            (Bound::Included(l), Bound::Unbounded) => write!(f, "({:?},)", l),
            (Bound::Included(l), Bound::Excluded(r)) => write!(f, "({:?}, {:?}]", l, r),
            (Bound::Included(l), Bound::Included(r)) => write!(f, "({:?}, {:?})", l, r),
            (Bound::Unbounded, Bound::Unbounded) => write!(f, "()"),
            (Bound::Unbounded, Bound::Excluded(r)) => write!(f, "(,{:?}]", r),
            (Bound::Unbounded, Bound::Included(r)) => write!(f, "(,{:?})", r),
        }?;

        write!(f, " with prefix {:?}", self.prefix)
    }
}
