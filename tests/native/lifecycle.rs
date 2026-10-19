use super::*;

#[tokio::test]
async fn root_creation_reclaims_cached_nodes() -> io::Result<()> {
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let empty_size = File::Node(Node::Leaf(vec![])).get_size();
        let cache = Cache::<File>::new(2 * empty_size, None, 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        for name in ["first", "second", "third"] {
            let dir = root.write().await.create_dir(name.into())?;
            let tree =
                BTreeLock::create(ExampleSchema::<i16>::new(1), Collator::default(), dir).await?;
            assert!(tree.read().await.is_empty(Range::<i16>::default()).await?);
        }

        root.sync().await?;
        drop(root);

        let root = Cache::<File>::new(BLOCK_SIZE, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        for name in ["first", "second", "third"] {
            let dir = root.read().await.get_dir(name).unwrap().clone();
            let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Collator::default(), dir)?;
            tree.validate().await?;
            assert!(tree.read().await.is_empty(Range::<i16>::default()).await?);
        }
    }

    fixture.close()
}

#[tokio::test]
async fn missing_root_returns_errors_without_repair() -> io::Result<()> {
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let cache = Cache::<File>::new(BLOCK_SIZE, None, 0, std::time::Duration::from_secs(1));
        let dir = cache.load(path.clone())?;
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(1),
            Collator::default(),
            dir.clone(),
        )
        .await?;
        tree.write().await.insert(vec![1]).await?;
        assert!(dir.write().await.delete(&uuid::Uuid::nil()).await);

        for operation in [
            "contains",
            "count",
            "first",
            "last",
            "is_empty",
            "keys",
            "keys_rev",
            "groups",
            "groups_rev",
            "insert",
            "insert_sorted",
            "replace",
            "delete",
            "validate",
        ] {
            let range = Range::<i16>::default();
            let result = match operation {
                "contains" => tree.read().await.contains(&[1]).await.map(|_| ()),
                "count" => tree.read().await.count(&range).await.map(|_| ()),
                "first" => tree.read().await.first(range).await.map(|_| ()),
                "last" => tree.read().await.last(range).await.map(|_| ()),
                "is_empty" => tree.read().await.is_empty(range).await.map(|_| ()),
                "keys" | "keys_rev" | "groups" | "groups_rev" => {
                    let reader = tree.read().await;
                    let stream = match operation {
                        "keys" => reader.keys(range).await,
                        "keys_rev" => reader.keys_rev(range).await,
                        "groups" => reader.groups(range, 1, false).await,
                        "groups_rev" => reader.groups(range, 1, true).await,
                        _ => unreachable!(),
                    };
                    match stream {
                        Ok(mut stream) => stream.try_next().await.map(|_| ()),
                        Err(error) => Err(error),
                    }
                }
                "insert" => tree.write().await.insert(vec![2]).await.map(|_| ()),
                "replace" => tree.write().await.replace(&[1], vec![2]).await.map(|_| ()),
                "insert_sorted" => tree
                    .write()
                    .await
                    .insert_sorted(futures::stream::iter([Ok(vec![2])]))
                    .await
                    .map(|_| ()),
                "delete" => tree.write().await.delete(&[1]).await.map(|_| ()),
                "validate" => tree.validate().await,
                _ => unreachable!(),
            };
            assert_eq!(
                result.expect_err(operation).kind(),
                io::ErrorKind::NotFound,
                "{operation}",
            );
            assert!(dir.read().await.is_empty(), "{operation} repaired storage");
        }

        drop(tree);
        drop(dir);
    }

    fixture.close()
}

#[tokio::test]
async fn interrupted_truncate_does_not_publish_a_missing_root() -> io::Result<()> {
    for cancel in [false, true] {
        let fixture = TestDir::new("test_btree")?;
        let path = fixture.path().to_owned();

        {
            let empty_size = File::Node(Node::Leaf(vec![])).get_size();
            let cache =
                Cache::<File>::new(empty_size, None, 0, std::time::Duration::from_millis(30));
            let root = cache.load(path.clone())?;
            let dir = root.write().await.create_dir("tree".into())?;
            let tree = BTreeLock::create(
                ExampleSchema::<i16>::new(1),
                Collator::default(),
                dir.clone(),
            )
            .await?;
            let filler = root
                .write()
                .await
                .create_empty_file("pinned".into(), File::Node(Node::Leaf(vec![])))
                .await?;
            let pinned = filler.read::<Node<Vec<Vec<i16>>>>().await?;
            {
                let mut writer = tree.write().await;
                let truncate = writer.truncate();
                futures::pin_mut!(truncate);
                assert!(futures::poll!(&mut truncate).is_pending());
                if !cancel {
                    assert_eq!(
                        truncate.await.unwrap_err().kind(),
                        io::ErrorKind::ResourceBusy
                    );
                }
            }
            // Native mutation may already have removed the old root. Failure and
            // cancellation must remain visible to strict loading, not be repaired.
            assert!(dir.read().await.is_empty());
            assert!(
                BTreeLock::load(
                    ExampleSchema::<i16>::new(1),
                    Collator::<i16>::default(),
                    dir,
                )
                .is_err()
            );
            drop(pinned);
            root.write().await.truncate_and_sync().await?;
        }

        fixture.close()?;
    }

    Ok(())
}

#[tokio::test]
async fn sorted_ingestion_retains_last_input_across_equal_collator_splits() -> io::Result<()> {
    #[derive(Clone, Eq, PartialEq)]
    struct Equal;

    impl collate::Collate for Equal {
        type Value = i16;

        fn cmp(&self, _: &i16, _: &i16) -> std::cmp::Ordering {
            let _ = COLLATIONS.try_with(|count| count.set(count.get() + 1));
            std::cmp::Ordering::Equal
        }
    }

    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1));
        let dir = cache.load(path.clone())?;
        let schema = ExampleSchema::<i16>::new(1);
        let order = schema.order() as i16;
        let tree = BTreeLock::create(schema, Equal, dir.clone()).await?;
        let keys = (0..=order).chain([order]).map(|i| Ok(vec![i]));

        assert_eq!(
            tree.write()
                .await
                .insert_sorted(futures::stream::iter(keys))
                .await?,
            (order + 1) as u64
        );

        let mut actual: Vec<_> = tree
            .read()
            .await
            .keys(Range::<i16>::default())
            .await?
            .map_ok(|key| key[0])
            .try_collect()
            .await?;
        actual.sort();
        assert_eq!(actual, (0..=order).collect::<Vec<_>>());
        for key in 0..=order {
            assert!(tree.read().await.contains(&[key]).await?);
            assert!(!tree.write().await.insert(vec![key]).await?);
        }
        assert_eq!(
            tree.write()
                .await
                .insert_sorted(futures::stream::iter([Ok(vec![0]), Ok(vec![order])]))
                .await?,
            0
        );
        assert!(tree.write().await.replace(&[0], vec![100]).await?);
        assert!(!tree.write().await.replace(&[order + 1], vec![101]).await?);
        assert_eq!(
            tree.write()
                .await
                .replace(&[100], vec![order])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        for key in order + 1..40 {
            assert!(tree.write().await.insert(vec![key]).await?);
        }
        tree.validate().await?;

        let mut leaves = 0;
        let mut interior = None;
        {
            let dir = dir.read().await;
            for (name, _) in dir.iter() {
                let file = dir.get_file(name).unwrap();
                if let Node::Leaf(rows) = &*file.read::<Node<Vec<Vec<i16>>>>().await? {
                    leaves += 1;
                    if rows.len() > 2 {
                        interior = Some(rows[1][0]);
                    }
                }
            }
        }

        let lookup_comparisons = COLLATIONS
            .scope(std::cell::Cell::new(0), async {
                LEAF_READS
                    .scope(std::cell::Cell::new(0), async {
                        assert_eq!(tree.read().await.get(&[20]).await?, Some(smallvec![20]));
                        assert_eq!(
                            LEAF_READS.with(std::cell::Cell::get),
                            leaves,
                            "each leaf is acquired once"
                        );
                        Ok::<_, io::Error>(COLLATIONS.with(std::cell::Cell::get))
                    })
                    .await
            })
            .await?;

        // An interior row adds only the two mathematical neighbor comparisons;
        // boundary replacements separately validate their routing separators.
        let interior = interior.expect("an interior row in the first split leaf");
        COLLATIONS
            .scope(std::cell::Cell::new(0), async {
                assert!(
                    tree.write()
                        .await
                        .replace(&[interior], vec![interior])
                        .await?
                );
                assert_eq!(
                    COLLATIONS.with(std::cell::Cell::get),
                    lookup_comparisons + 2,
                    "one group scan and two neighbor comparisons"
                );
                Ok::<_, io::Error>(())
            })
            .await?;

        // Absent-target behavior wins even if the supplied replacement would collide.
        assert!(!tree.write().await.replace(&[101], vec![20]).await?);
        tree.sync_all().await?;
        drop(tree);
        let dir = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Equal, dir)?;
        tree.validate().await?;
        for key in (1..40).chain([100]) {
            assert!(tree.read().await.contains(&[key]).await?, "missing {key}");
            assert!(tree.write().await.delete(&[key]).await?, "delete {key}");
        }
        assert_eq!(tree.read().await.count(&Range::<i16>::default()).await?, 0);

        drop(tree);
    }

    fixture.close()
}
