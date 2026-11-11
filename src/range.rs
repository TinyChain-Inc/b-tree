use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::{Bound, Range as Bounds};
use std::{fmt, io};

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
        !matches!(
            (&self.start, &self.end),
            (Bound::Unbounded, Bound::Unbounded)
        )
    }

    /// Return `true` if this [`Range`] is empty.
    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty()
            && matches!(
                (&self.start, &self.end),
                (Bound::Unbounded, Bound::Unbounded)
            )
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

impl<V> Range<V> {
    pub(super) fn validate<C>(&self, columns: usize, collator: &Collator<C>) -> io::Result<()>
    where
        V: Borrow<C::Value>,
        C: Collate,
    {
        if self.len() > columns {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BTree range references a column outside its schema",
            ));
        }

        match (&self.start, &self.end) {
            (
                Bound::Included(start) | Bound::Excluded(start),
                Bound::Included(end) | Bound::Excluded(end),
            ) if collator.value.cmp(start.borrow(), end.borrow()).is_gt() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "BTree range starts after its end",
                ));
            }
            _ => {}
        }

        Ok(())
    }

    // Routing treats a separator's omitted suffix as negative infinity. Query
    // endpoints use a virtual suffix as well, without allocating sentinel values.
    pub(crate) fn cmp_endpoint<C>(
        &self,
        separator: &[C::Value],
        lower: bool,
        collator: &Collator<C>,
    ) -> Ordering
    where
        V: Borrow<C::Value>,
        C: Collate,
    {
        let bound = if lower { &self.start } else { &self.end };
        let maximum = matches!(
            (lower, bound),
            (true, Bound::Excluded(_)) | (false, Bound::Included(_) | Bound::Unbounded)
        );
        let endpoint_len = self.prefix.len() + usize::from(!matches!(bound, Bound::Unbounded));

        for (i, value) in separator.iter().enumerate() {
            let endpoint = if let Some(prefix) = self.prefix.get(i) {
                prefix.borrow()
            } else if i == self.prefix.len() {
                match bound {
                    Bound::Included(value) | Bound::Excluded(value) => value.borrow(),
                    Bound::Unbounded => {
                        return if maximum {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        };
                    }
                }
            } else {
                return if maximum {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            };

            let order = collator.value.cmp(value, endpoint);
            if !order.is_eq() {
                return order;
            }
        }

        if separator.len() < endpoint_len || maximum {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }

    pub(crate) fn child_slots<C>(
        &self,
        separators: &[Vec<C::Value>],
        collator: &Collator<C>,
    ) -> (usize, usize)
    where
        V: Borrow<C::Value>,
        C: Collate,
    {
        let start =
            separators.partition_point(|row| self.cmp_endpoint(row, true, collator).is_lt());
        let end =
            separators.partition_point(|row| !self.cmp_endpoint(row, false, collator).is_gt()) + 1;
        (start, end)
    }
}

impl<BV> Range<BV> {
    fn overlaps_key<C>(&self, key: &[C::Value], collator: &Collator<C>) -> Overlap
    where
        BV: Borrow<C::Value>,
        C: Collate,
    {
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
                    (_, Ordering::Less) => Overlap::Less,
                    (Ordering::Greater, _) => Overlap::Greater,
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

impl<BV, C> OverlapsValue<Vec<C::Value>, Collator<C>> for Range<BV>
where
    BV: Borrow<C::Value>,
    C: Collate,
    C::Value: fmt::Debug,
{
    fn overlaps_value(&self, key: &Vec<C::Value>, collator: &Collator<C>) -> Overlap {
        self.overlaps_key(key, collator)
    }
}

impl<C: Collate> OverlapsRange<Range<C::Value>, Collator<C>> for Range<C::Value> {
    fn overlaps(&self, other: &Range<C::Value>, collator: &Collator<C>) -> Overlap {
        match collator.cmp_slices(&self.prefix, &other.prefix) {
            Ordering::Less => Overlap::Less,
            Ordering::Greater => Overlap::Greater,
            Ordering::Equal => match self.prefix.len().cmp(&other.prefix.len()) {
                Ordering::Less => match self.overlaps_key(&other.prefix, collator) {
                    Overlap::Less => Overlap::Less,
                    Overlap::Greater => Overlap::Greater,
                    _ => Overlap::Wide,
                },
                Ordering::Equal => {
                    (&self.start, &self.end).overlaps(&(&other.start, &other.end), &collator.value)
                }
                Ordering::Greater => match other.overlaps_key(&self.prefix, collator) {
                    Overlap::Less => Overlap::Greater,
                    Overlap::Greater => Overlap::Less,
                    _ => Overlap::Narrow,
                },
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

#[cfg(test)]
#[path = "../tests/unit/range.rs"]
mod tests;
