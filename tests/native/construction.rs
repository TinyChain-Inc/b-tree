use super::*;

#[tokio::test]
async fn sorted_ingestion_matches_insertion_and_survives_errors() -> Result<(), io::Error> {
    use futures::{StreamExt, stream};

    #[derive(Clone, Eq, PartialEq)]
    struct Counted;

    impl collate::Collate for Counted {
        type Value = i16;

        fn cmp(&self, left: &i16, right: &i16) -> std::cmp::Ordering {
            let _ = COLLATIONS.try_with(|count| count.set(count.get() + 1));
            left.cmp(right)
        }
    }

    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
        let dir = cache.load(path.clone())?;
        let tree = BTreeLock::create(ExampleSchema::<i16>::new(1), Counted, dir).await?;
        {
            let mut write = tree.write().await;
            assert_eq!(write.insert_sorted(stream::empty()).await?, 0);
            write.insert(vec![0]).await?;
            assert_eq!(
                write
                    .insert_sorted(stream::iter(
                        (0..2000).flat_map(|i| [Ok(vec![i]), Ok(vec![i])])
                    ))
                    .await?,
                1999
            );
            assert!(
                write
                    .insert_sorted(stream::iter([Ok(vec![1998])]))
                    .await
                    .is_err()
            );
            assert!(
                write
                    .insert_sorted(stream::iter([
                        Ok(vec![2000]),
                        Err(io::Error::other("source"))
                    ]))
                    .await
                    .is_err()
            );
            assert!(
                write
                    .insert_sorted(stream::iter([Ok(vec![2001, 0])]))
                    .await
                    .is_err()
            );
            let (send, receive) = tokio::sync::oneshot::channel();
            let mut send = Some(send);
            let pending = stream::iter([Ok(vec![2001])]).chain(stream::poll_fn(move |_| {
                if let Some(send) = send.take() {
                    send.send(()).unwrap();
                }
                std::task::Poll::Pending
            }));
            {
                let consume = write.insert_sorted(pending);
                futures::pin_mut!(consume);
                tokio::select! {
                    result = &mut consume => panic!("pending source completed: {result:?}"),
                    result = receive => result.unwrap(),
                }
            }
            assert!(write.contains(&[2001]).await?);
        }
        tree.validate().await?;
        tree.sync_all().await?;
        drop(tree);
        let reopened = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3))
            .load(path.clone())?;
        let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Counted, reopened)?;
        let actual: Vec<_> = tree
            .read()
            .await
            .keys(Range::<i16>::default())
            .await?
            .map_ok(|key| key[0])
            .try_collect()
            .await?;
        assert_eq!(actual, (0..2002).collect::<Vec<_>>());

        for (start, end) in [(0, 0), (3, 17), (1995, 2002), (2002, 2010)] {
            for reverse in [false, true] {
                let range = Range::with_range(vec![], start..end);
                let reader = tree.read().await;
                let keys = if reverse {
                    reader.keys_rev(range).await?
                } else {
                    reader.keys(range).await?
                };
                let actual: Vec<_> = keys.map_ok(|key| key[0]).try_collect().await?;
                let mut expected: Vec<_> = (start..end.min(2002)).collect();
                if reverse {
                    expected.reverse();
                }

                assert_eq!(actual, expected);
            }
        }

        // A partially consumed stream owns its guard; dropping it releases the writer.
        for reverse in [false, true] {
            let reader = tree.read().await;
            let mut keys = if reverse {
                reader.keys_rev(Range::<i16>::default()).await?
            } else {
                reader.keys(Range::<i16>::default()).await?
            };
            assert!(keys.try_next().await?.is_some());
            assert!(tree.try_write().is_err());

            drop(keys);
            drop(tree.try_write()?);
        }

        {
            let mut write = tree.write().await;
            for key in (0..2002).step_by(2) {
                assert!(write.delete(&[key]).await?);
            }
            for key in (0..2002).step_by(2) {
                assert!(write.insert(vec![key]).await?);
            }
            for key in (0..2002).rev() {
                assert!(write.delete(&[key]).await?);
            }
        }
        tree.validate().await?;

        // The empty root fits all three rows: only the two previous-input comparisons
        // run, with no split, ancestor search, or equivalent-group membership lookup.
        COLLATIONS
            .scope(std::cell::Cell::new(0), async {
                assert_eq!(
                    tree.write()
                        .await
                        .insert_sorted(stream::iter((0..3).map(|key| Ok(vec![key]))))
                        .await?,
                    3
                );
                assert_eq!(COLLATIONS.with(std::cell::Cell::get), 2);
                Ok::<_, io::Error>(())
            })
            .await?;

        drop(tree);
    }

    fixture.close()
}

#[tokio::test]
async fn duplicate_insertion_preserves_retained_allocation() -> io::Result<()> {
    use futures::stream;
    use uuid::Uuid;

    for count in [1_i16, 5, 8] {
        let fixture = TestDir::new("test_btree")?;
        let path = fixture.path().to_owned();

        {
            let entries = if count == 8 {
                // Three levels exercise both recursive descent and root forwarding.
                let mut entries = vec![
                    (
                        Uuid::nil(),
                        Node::Index(vec![vec![4]], vec![Uuid::from_u128(1), Uuid::from_u128(2)]),
                    ),
                    (
                        Uuid::from_u128(1),
                        Node::Index(vec![vec![2]], vec![Uuid::from_u128(3), Uuid::from_u128(4)]),
                    ),
                    (
                        Uuid::from_u128(2),
                        Node::Index(vec![vec![6]], vec![Uuid::from_u128(5), Uuid::from_u128(6)]),
                    ),
                ];
                for i in 0..4 {
                    entries.push((
                        Uuid::from_u128(3 + i as u128),
                        Node::Leaf(vec![vec![2 * i], vec![2 * i + 1]]),
                    ));
                }
                entries
            } else {
                vec![(
                    Uuid::nil(),
                    Node::Leaf((0..count).map(|i| vec![i]).collect()),
                )]
            };
            let retained = entries.iter().map(|(_, node)| node.get_size()).sum();
            if count == 1 {
                assert_eq!(retained, 74);
            }
            let cache = Cache::<File>::new(retained, None, 0, std::time::Duration::from_secs(1));
            let dir = cache.clone().load(path.clone())?;
            for (id, node) in entries {
                dir.write().await.create_file(id.to_string(), node).await?;
            }
            let mut schema = ExampleSchema::<i16>::new(1);
            if count == 8 {
                schema.order = 4;
            }
            let tree = BTreeLock::load(schema, Collator::default(), dir.clone())?;
            tree.validate().await?;
            {
                let mut write = tree.write().await;
                for i in 0..count {
                    assert!(!write.insert(vec![i]).await?);
                    // Unused input capacity must not become cache growth for a no-op.
                    let mut duplicate = Vec::with_capacity(1_000_000);
                    duplicate.push(i);
                    assert!(!write.insert(duplicate).await?);
                }
                // Replacement always publishes the supplied row; only insertion
                // duplicates retain the old allocation without mutation.
                assert!(write.replace(&[0], vec![0]).await?);
                // The collator value changes, but the first row keeps its position
                // before its successor. Replacement does not require collation equality.
                assert!(write.replace(&[0], vec![-1]).await?);
                assert!(write.replace(&[-1], vec![0]).await?);
                let mut duplicate = Vec::with_capacity(1_000_000);
                duplicate.push(count - 1);
                assert_eq!(write.insert_sorted(stream::iter([Ok(duplicate)])).await?, 0);
                assert_eq!(write.count(&Range::<i16>::default()).await?, count as u64);
                assert!(!write.contains(&[count]).await?);
            }
            tree.validate().await?;
            tree.sync_all().await?;
            drop(tree);
            drop(dir);
            drop(cache);

            // Decoding has its own admitted scratch bound, independent of the
            // exactly full resident cache used to exercise duplicate insertion.
            let reopened =
                Cache::<File>::new(1_000_000, None, 0, std::time::Duration::from_secs(1))
                    .load(path.clone())?;
            let mut schema = ExampleSchema::<i16>::new(1);
            if count == 8 {
                schema.order = 4;
            }
            let tree = BTreeLock::load(schema, Collator::default(), reopened)?;
            tree.validate().await?;
            let keys: Vec<_> = tree
                .read()
                .await
                .keys(Range::<i16>::default())
                .await?
                .map_ok(|key| key[0])
                .try_collect()
                .await?;
            assert_eq!(keys, (0..count).collect::<Vec<_>>());
            drop(tree);
        }

        fixture.close()?;
    }

    Ok(())
}
