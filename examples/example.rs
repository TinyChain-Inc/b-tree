use std::io;

use collate::Collator;
use freqfs::Cache;
use futures::TryStreamExt;
use rand::RngExt;
use smallvec::smallvec;

use self::adapters::{ExampleSchema, File};
use self::directory::TestDir;
use b_tree::{BTreeLock, Range, Schema};

#[path = "support/adapters.rs"]
mod adapters;
#[path = "../tests/common/directory.rs"]
mod directory;

async fn functional_test() -> Result<(), io::Error> {
    // set up the test directory
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        // construct the schema
        let schema = ExampleSchema::<i16>::new(3);

        // Exercise functional behavior; dedicated tests cover cache pressure.
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));

        // load the directory and file paths into memory (not file contents, yet)
        let dir = cache.load(path.clone())?;

        // create a new B+ tree
        let btree = BTreeLock::create(schema, Collator::<i16>::default(), dir).await?;

        let default_range = Range::<i16>::default();

        let n = 300;

        {
            let mut view = btree.write().await;

            assert!(view.is_empty(&default_range).await?);
            assert_eq!(view.count(&default_range).await?, 0);

            for i in 1..n {
                let lo = i;
                let hi = i16::MAX - lo;
                let spread = hi - lo;

                let key = vec![lo, hi, spread];

                assert!(!view.contains(&key).await?);
                assert!(view.is_empty(&Range::from_prefix(vec![i])).await?);

                assert!(view.insert(key.clone()).await?);

                assert!(view.contains(&key).await?);
                assert!(!view.is_empty(Range::from_prefix(vec![i])).await?);

                assert_eq!(
                    view.count(&Range::with_range(smallvec![], 0..i)).await?,
                    (i as u64) - 1
                );

                assert_eq!(view.count(&Range::from_prefix(vec![i])).await?, 1);
                assert_eq!(view.count(&default_range).await?, i as u64);
            }
        }

        btree.validate().await?;
        {
            let view = btree.read().await;
            let streamed = {
                let mut keys = view.clone().keys(default_range.clone()).await?;
                let mut count = 0;
                while keys.try_next().await?.is_some() {
                    count += 1;
                }
                count
            };
            assert_eq!(view.count(&default_range).await?, streamed);

            let mut i = 1;

            {
                let range = Range::with_range(vec![], 0..67);
                let mut keys = view.clone().keys(range).await?;
                while let Some(key) = keys.try_next().await? {
                    assert_eq!(key[0], i);
                    i += 1;
                }
            }

            {
                let range = Range::with_range(vec![], 67..250);
                let mut keys = view.clone().keys(range).await?;
                while let Some(key) = keys.try_next().await? {
                    assert_eq!(key[0], i);
                    i += 1;
                }
            }

            let mut i = 1;

            {
                let mut keys = view.clone().keys(Range::with_range(vec![], 0..123)).await?;

                while let Some(key) = keys.try_next().await? {
                    assert_eq!(key[0], i);
                    i += 1;
                }
            }

            {
                let mut keys = view.keys(Range::with_range(vec![], 123..n)).await?;
                while let Some(key) = keys.try_next().await? {
                    assert_eq!(key[0], i);
                    i += 1;
                }
            }

            let view = btree.read().await;
            assert_eq!(view.count(&Range::<i16>::default()).await?, (n - 1) as u64);

            for i in 1..n {
                let count = (i as u64) - 1;
                let range_left = Range::with_range(vec![], 0..i);
                assert_eq!(view.count(&range_left).await?, count, "bad count at {}", i);
            }

            std::mem::drop(view);

            let view = btree.read().await;

            for i in 1..n {
                let key = vec![i, i16::MAX - i, i16::MAX - 2 * i];
                assert!(view.contains(&key).await?);
            }

            {
                let mut i = n - 1;
                let mut reversed = view.clone().keys_rev(Range::<i16>::default()).await?;
                while let Some(key) = reversed.try_next().await? {
                    assert_eq!(key[0], i);
                    i -= 1;
                }
                assert_eq!(i, 0);
            }

            {
                let mut groups = view
                    .clone()
                    .groups(Range::from_prefix(smallvec![1]), 2, false)
                    .await?;

                assert_eq!(groups.try_next().await?, Some(smallvec![1, i16::MAX - 1]));
                assert_eq!(groups.try_next().await?, None);

                let mut i = 1i16;
                let mut groups = view
                    .clone()
                    .groups(Range::<i16>::default(), 1, false)
                    .await?;

                while let Some(group) = groups.try_next().await? {
                    assert_eq!(group.as_slice(), &[i]);
                    i += 1;
                }

                let mut groups = view
                    .clone()
                    .groups(Range::<i16>::default(), 1, true)
                    .await?;

                while let Some(group) = groups.try_next().await? {
                    i -= 1;
                    assert_eq!(group.as_slice(), &[i]);
                }
            }

            std::mem::drop(view);

            let mut view = btree.write().await;
            let mut count = view.count(&Range::<i16>::default()).await?;
            assert_eq!(count, (n - 1) as u64);
            assert!(!view.is_empty(&Range::<i16>::default()).await?);

            while !view.is_empty(&default_range).await? {
                let lo = view.first(Range::<i16>::default()).await?.expect("first")[0];
                let hi = view.last(Range::<i16>::default()).await?.expect("last")[0];

                let i = rand::rng().random_range(lo..(hi + 1));
                let key = [i, i16::MAX - i, i16::MAX - 2 * i];

                let present = view.contains(&key).await?;

                assert_eq!(present, view.delete(&key).await?);
                assert!(!view.contains(&key).await?);

                if present {
                    count -= 1;
                }

                assert_eq!(view.count(&Range::<i16>::default()).await?, count);
            }
        }

        btree.validate().await?;
        {
            let view = btree.try_read().expect("btree read");
            let count = view.count(&default_range).await?;
            let mut keys = view.keys(default_range).await?;
            assert_eq!(keys.try_next().await?, None);
            assert_eq!(count, 0);
        }

        // clean up
    }

    fixture.close()
}

async fn load_test() -> Result<(), io::Error> {
    let n = 100_000;

    // set up the test directory
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        // construct the schema
        let schema = ExampleSchema::<i16>::new(3);

        // initialize the cache
        let cache = Cache::<File>::new(
            schema.block_size() * n,
            None,
            0,
            std::time::Duration::from_secs(3),
        );

        // load the directory and file paths into memory (not file contents, yet)
        let dir = cache.load(path.clone())?;

        // create a new B+ tree
        let btree = BTreeLock::create(schema, Collator::<i16>::default(), dir).await?;

        {
            let mut view = btree.write().await;

            assert!(view.is_empty(&Range::<i16>::default()).await?);
            assert_eq!(view.count(&Range::<i16>::default()).await?, 0);

            for _ in 0..(n / 2) {
                let i: i16 = rand::rng().random_range(i16::MIN..i16::MAX);
                let key = vec![i, i / 2, i % 2];
                view.insert(key).await?;
            }

            for _ in (n / 2)..n {
                let i: i16 = rand::rng().random_range(i16::MIN..i16::MAX);
                let key = vec![i, i / 2, i % 2];
                view.insert(key).await?;

                let i: i16 = rand::rng().random_range(i16::MIN..i16::MAX);
                let key = [i, i / 2, i % 2];
                view.delete(&key).await?;
            }

            for _ in 0..(n / 2) {
                let i: i16 = rand::rng().random_range(i16::MIN..i16::MAX);
                let key = [i, i / 2, i % 2];
                view.delete(&key).await?;
            }
        }

        // clean up
    }

    fixture.close()
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    functional_test().await?;
    load_test().await?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/common/observations.rs"]
mod observations;
