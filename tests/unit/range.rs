use std::cmp::Ordering;
use std::ops::Bound::{Excluded, Included, Unbounded};

use smallvec::smallvec;

use super::Range;
use crate::Collator;

#[test]
fn hierarchical_range_overlap_uses_value_boundaries() {
    use collate::{Overlap, OverlapsRange, OverlapsValue};

    let collator = Collator::new(collate::Collator::<i16>::default());
    for (name, parent, child, forward, reverse) in [
        (
            "all rows",
            Range::default(),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Wide,
            Overlap::Narrow,
        ),
        (
            "prefix only",
            Range::from_prefix(smallvec![2]),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Wide,
            Overlap::Narrow,
        ),
        (
            "included lower endpoint",
            Range::with_bounds(smallvec![2], (Included(3), Unbounded)),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Wide,
            Overlap::Narrow,
        ),
        (
            "excluded lower endpoint",
            Range::with_bounds(smallvec![2], (Excluded(3), Unbounded)),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Greater,
            Overlap::Less,
        ),
        (
            "included upper endpoint",
            Range::with_bounds(smallvec![2], (Unbounded, Included(3))),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Wide,
            Overlap::Narrow,
        ),
        (
            "excluded upper endpoint",
            Range::with_bounds(smallvec![2], (Unbounded, Excluded(3))),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Less,
            Overlap::Greater,
        ),
        (
            "equal finite endpoints",
            Range::with_bounds(smallvec![2], (Included(3), Included(3))),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Wide,
            Overlap::Narrow,
        ),
        (
            "different prefixes",
            Range::from_prefix(smallvec![1]),
            Range::from_prefix(smallvec![2, 3]),
            Overlap::Less,
            Overlap::Greater,
        ),
        (
            "bounded descendant",
            Range::with_bounds(smallvec![2], (Included(3), Excluded(8))),
            Range::with_bounds(smallvec![2, 5], (Included(10), Excluded(20))),
            Overlap::Wide,
            Overlap::Narrow,
        ),
    ] {
        assert_eq!(parent.overlaps(&child, &collator), forward, "{name}");
        assert_eq!(child.overlaps(&parent, &collator), reverse, "{name}");
        assert_eq!(
            parent.contains(&child, &collator),
            forward == Overlap::Wide,
            "{name}"
        );
        assert_eq!(
            parent.contains_partial(&child, &collator),
            forward == Overlap::Wide,
            "{name}"
        );
        assert!(!child.contains(&parent, &collator), "{name}");
        assert_eq!(
            parent.overlaps(&parent, &collator),
            Overlap::Equal,
            "{name}"
        );

        let prefix = child.prefix.to_vec();
        let owned = parent.overlaps_value(&prefix, &collator);
        assert_eq!(
            parent.as_ref().overlaps_value(&prefix, &collator),
            owned,
            "{name}"
        );
        assert_eq!(parent.overlaps_key(&prefix, &collator), owned, "{name}");
    }

    let parent = Range::with_bounds(smallvec![2], (Included(3), Excluded(8)));
    let child = Range::with_bounds(smallvec![2], (Included(4), Excluded(6)));
    assert_eq!(parent.overlaps(&child, &collator), Overlap::Wide);
    assert_eq!(child.overlaps(&parent, &collator), Overlap::Narrow);
}

#[test]
fn compact_separators_route_virtual_range_endpoints() {
    let collator = Collator::new(collate::Collator::<i16>::default());
    let separators = vec![vec![1], vec![2, 3], vec![2, 3], vec![2, 8], vec![4]];
    let cases = [
        ("all", Range::default(), (0, 6)),
        ("short prefix", Range::from_prefix(smallvec![2]), (1, 5)),
        (
            "tied full prefix",
            Range::from_prefix(smallvec![2, 3]),
            (1, 4),
        ),
        (
            "inclusive tied endpoints",
            Range::with_bounds(smallvec![2], (Included(3), Included(3))),
            (1, 4),
        ),
        (
            "exclusive lower endpoint",
            Range::with_bounds(smallvec![2], (Excluded(3), Unbounded)),
            (3, 5),
        ),
        (
            "exclusive upper endpoint",
            Range::with_bounds(smallvec![2], (Unbounded, Excluded(3))),
            (1, 4),
        ),
        (
            "upper endpoint before first matching separator",
            Range::with_bounds(smallvec![2], (Unbounded, Excluded(2))),
            (1, 2),
        ),
        ("between prefixes", Range::from_prefix(smallvec![3]), (4, 5)),
        (
            "before first separator",
            Range::from_prefix(smallvec![0]),
            (0, 1),
        ),
        (
            "after last separator",
            Range::from_prefix(smallvec![5]),
            (5, 6),
        ),
    ];

    for (name, range, expected) in cases {
        assert_eq!(
            range.child_slots(&separators, &collator),
            expected,
            "{name}"
        );
    }
}

#[test]
fn compact_separator_suffix_is_not_a_prefix_wildcard() {
    let collator = Collator::new(collate::Collator::<i16>::default());
    assert_eq!(
        collate::CollateRef::cmp_ref(&collator, &[7][..], &[7, 42][..]),
        Ordering::Equal
    );
    assert_eq!(collator.cmp_routing(&[7], &[7, 42]), Ordering::Less);
    assert_eq!(collator.cmp_routing(&[7, 42], &[7]), Ordering::Greater);

    let range = Range::with_bounds(smallvec![2], (Included(3), Excluded(8)));
    let cases: &[(&str, &[i16], Ordering, Ordering)] = &[
        ("short separator", &[2], Ordering::Less, Ordering::Less),
        ("lower boundary", &[2, 3], Ordering::Equal, Ordering::Less),
        (
            "finite lower suffix",
            &[2, 3, 9],
            Ordering::Greater,
            Ordering::Less,
        ),
        (
            "upper boundary",
            &[2, 8],
            Ordering::Greater,
            Ordering::Equal,
        ),
        (
            "finite upper suffix",
            &[2, 8, 0],
            Ordering::Greater,
            Ordering::Greater,
        ),
    ];

    for &(name, separator, lower, upper) in cases {
        assert_eq!(
            range.cmp_endpoint(separator, true, &collator),
            lower,
            "{name}"
        );
        assert_eq!(
            range.cmp_endpoint(separator, false, &collator),
            upper,
            "{name}"
        );
    }
}

#[test]
fn separator_clones_only_retained_columns() {
    use std::cell::Cell;
    use std::rc::Rc;

    struct Observed(u64, Rc<Cell<usize>>);

    impl Clone for Observed {
        fn clone(&self) -> Self {
            self.1.set(self.1.get() + 1);
            Self(self.0, self.1.clone())
        }
    }

    #[derive(Eq, PartialEq)]
    struct Numeric;

    impl collate::Collate for Numeric {
        type Value = Observed;

        fn cmp(&self, left: &Observed, right: &Observed) -> Ordering {
            left.0.cmp(&right.0)
        }
    }

    let keys = Rc::new(Cell::new(0));
    let payloads = Rc::new(Cell::new(0));
    let collator = Collator::new(Numeric);
    let left = [Observed(1, keys.clone()), Observed(10, payloads.clone())];
    let right = [Observed(2, keys.clone()), Observed(20, payloads.clone())];
    assert_eq!(collator.separator(&left, &right).len(), 1);
    assert_eq!((keys.get(), payloads.get()), (1, 0));

    let tied = [Observed(2, keys.clone()), Observed(20, payloads.clone())];
    assert_eq!(collator.separator(&right, &tied).len(), 2);
    assert_eq!((keys.get(), payloads.get()), (2, 1));
}
