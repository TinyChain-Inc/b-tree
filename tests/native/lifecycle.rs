use super::*;

#[tokio::test]
async fn ordinary_reads_reject_malformed_leaf_rows() -> io::Result<()> {
    let fixture = TestDir::new("test_btree_read_width")?;
    {
        let dir = Cache::<File>::new(1024 * 1024, Some(1), 0, std::time::Duration::from_secs(3))
            .load(fixture.path().to_owned())?;
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(2),
            Collator::default(),
            dir.clone(),
        )
        .await?;
        tree.write().await.insert(vec![1, 10]).await?;
        tree.write().await.insert(vec![2, 20]).await?;
        let root = dir
            .read()
            .await
            .get_file(
                uuid::Uuid::nil()
                    .hyphenated()
                    .encode_lower(&mut uuid::Uuid::encode_buffer()),
            )
            .unwrap()
            .clone();
        let check = |error: io::Error| {
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(freqfs::IntegrityError::is_marked(&error));
        };
        for malformed in [vec![1], vec![1, 10, 100]] {
            {
                let mut node = root.write::<Node<Vec<Vec<i16>>>>().await?;
                let Node::Leaf(rows) = &mut *node else {
                    panic!("expected leaf")
                };
                rows[0] = malformed;
            }
            for range in [Range::default(), Range::from_prefix(vec![2])] {
                check(tree.read().await.count(&range).await.unwrap_err());
                check(tree.read().await.first(range.clone()).await.unwrap_err());
                check(tree.read().await.last(range.clone()).await.unwrap_err());
                check(tree.read().await.is_empty(range.clone()).await.unwrap_err());
                for reverse in [false, true] {
                    for columns in [None, Some(vec![0])] {
                        let reader = tree.clone().into_read().await;
                        let rows = if let Some(columns) = columns {
                            reader.select(range.clone(), columns, reverse).await
                        } else if reverse {
                            reader.keys_rev(range.clone()).await
                        } else {
                            reader.keys(range.clone()).await
                        };
                        match rows {
                            Ok(mut rows) => check(rows.try_next().await.unwrap_err()),
                            Err(error) => check(error),
                        }
                        drop(tree.try_write()?);
                    }
                    let reader = tree.read().await;
                    let mut called = false;
                    let result = reader
                        .visit(range.clone(), reverse, |_| {
                            called = true;
                            Ok::<_, io::Error>(std::ops::ControlFlow::Continue(()))
                        })
                        .await;
                    check(result.unwrap_err());
                    assert!(!called);
                }
            }
            let before = match &*root.read::<Node<Vec<Vec<i16>>>>().await? {
                Node::Leaf(rows) => rows.clone(),
                Node::Index(..) => panic!("expected root leaf"),
            };
            check(
                tree.write()
                    .await
                    .insert_sorted(futures::stream::iter([Ok(vec![3, 30])]))
                    .await
                    .unwrap_err(),
            );
            let node = root.read::<Node<Vec<Vec<i16>>>>().await?;
            let Node::Leaf(rows) = &*node else {
                panic!("root changed kind")
            };
            assert_eq!(rows, &before);
            drop(node);
            drop(tree.try_write()?);
        }
        // Recheck after polling input: a stored-page fault can appear after
        // the initial rightmost descent and before the append guard is acquired.
        *root.write::<Node<Vec<Vec<i16>>>>().await? = Node::Leaf(vec![vec![1, 10], vec![2, 20]]);
        let input = futures::stream::once(async {
            let mut node = root.write::<Node<Vec<Vec<i16>>>>().await?;
            let Node::Leaf(rows) = &mut *node else {
                panic!("expected root leaf")
            };
            rows[0].pop();
            Ok::<_, io::Error>(vec![3, 30])
        });
        check(tree.write().await.insert_sorted(input).await.unwrap_err());
        let node = root.read::<Node<Vec<Vec<i16>>>>().await?;
        let Node::Leaf(rows) = &*node else {
            panic!("root changed kind")
        };
        assert_eq!(rows, &vec![vec![1], vec![2, 20]]);
        drop(node);
        drop(tree.try_write()?);
    }
    // Sorted appends must validate a malformed leaf below an otherwise valid root.
    {
        let path = TestDir::new("sorted_nested_width")?;
        {
            let dir =
                Cache::<File>::new(1024 * 1024, Some(1), 0, std::time::Duration::from_secs(3))
                    .load(path.path().to_owned())?;
            let tree = BTreeLock::create(
                ExampleSchema::<i16>::new(2),
                Collator::default(),
                dir.clone(),
            )
            .await?;
            let ids = [1, 2].map(uuid::Uuid::from_u128);
            {
                let mut native = dir.write().await;
                native
                    .create_file(
                        ids[0].to_string(),
                        Node::Leaf(vec![vec![1, 10], vec![2, 20]]),
                    )
                    .await?;
                native
                    .create_file(ids[1].to_string(), Node::Leaf(vec![vec![3], vec![4, 40]]))
                    .await?;
                *native
                    .write_file::<Node<Vec<Vec<i16>>>>(&uuid::Uuid::nil().to_string())
                    .await? = Node::Index(vec![vec![3]], ids.to_vec());
            }
            let error = tree
                .write()
                .await
                .insert_sorted(futures::stream::iter([Ok(vec![5, 50])]))
                .await
                .unwrap_err();
            assert!(freqfs::IntegrityError::is_marked(&error));
            let leaf = dir
                .read()
                .await
                .read_file::<Node<Vec<Vec<i16>>>>(&ids[1].to_string())
                .await?
                .clone();
            let Node::Leaf(rows) = leaf else {
                panic!("leaf changed kind")
            };
            assert_eq!(rows, vec![vec![3], vec![4, 40]]);
            drop(tree.try_write()?);
        }
        path.close()?;
    }
    fixture.close()
}

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
        assert!(
            dir.write()
                .await
                .delete(
                    uuid::Uuid::nil()
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer())
                )
                .await
        );

        for operation in [
            "contains",
            "get_with",
            "visit",
            "count",
            "first",
            "last",
            "is_empty",
            "keys",
            "keys_rev",
            "select",
            "select_rev",
            "insert",
            "insert_sorted",
            "replace",
            "delete",
            "validate",
        ] {
            let range = Range::<i16>::default();
            let result = match operation {
                "contains" => tree.read().await.contains(&[1]).await.map(|_| ()),
                "get_with" => tree
                    .read()
                    .await
                    .get_with(&[1], |_| Ok::<_, io::Error>(()))
                    .await
                    .map(|_| ()),
                "visit" => {
                    tree.read()
                        .await
                        .visit(range, false, |_| {
                            Ok::<_, io::Error>(std::ops::ControlFlow::Continue(()))
                        })
                        .await
                }
                "count" => tree.read().await.count(&range).await.map(|_| ()),
                "first" => tree.read().await.first(range).await.map(|_| ()),
                "last" => tree.read().await.last(range).await.map(|_| ()),
                "is_empty" => tree.read().await.is_empty(range).await.map(|_| ()),
                "keys" | "keys_rev" | "select" | "select_rev" => {
                    let reader = tree.read().await;
                    let stream = match operation {
                        "keys" => reader.keys(range).await,
                        "keys_rev" => reader.keys_rev(range).await,
                        "select" => reader.select(range, vec![0], false).await,
                        "select_rev" => reader.select(range, vec![0], true).await,
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
                .create_file("pinned".into(), File::Node(Node::Leaf(vec![])))
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

#[tokio::test]
async fn selected_columns_preserve_rows_order_and_guard_release() -> io::Result<()> {
    use std::ops::Bound::{Excluded, Included};

    let fixture = TestDir::new("test_btree_projection")?;
    {
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
        let dir = cache.load(fixture.path().to_owned())?;
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(3),
            Collator::default(),
            dir.clone(),
        )
        .await?;
        for key in 0..20 {
            tree.write().await.insert(vec![key, 1, key + 100]).await?;
        }

        for reverse in [false, true] {
            let range = Range::with_bounds(smallvec![], (Included(3i16), Excluded(17)));
            let mut rows = tree
                .read()
                .await
                .select(range, vec![1, 0, 1], reverse)
                .await?;
            let mut expected: Vec<_> = (3..17).collect();
            if reverse {
                expected.reverse();
            }
            for key in expected {
                assert_eq!(rows.try_next().await?.unwrap().as_slice(), &[1, key, 1]);
            }
            assert!(rows.try_next().await?.is_none());
            drop(rows);
        }

        let first_leaf = {
            let contents = dir.read().await;
            let mut id = uuid::Uuid::nil();
            loop {
                let file = contents
                    .get_file(
                        id.hyphenated()
                            .encode_lower(&mut uuid::Uuid::encode_buffer()),
                    )
                    .unwrap();
                let node = file.read::<Node<Vec<Vec<i16>>>>().await?;
                match &*node {
                    Node::Leaf(_) => break file.clone(),
                    Node::Index(_, children) => id = children[0],
                }
            }
        };
        let mut rows = tree
            .read()
            .await
            .select(Range::<i16>::default(), vec![1], false)
            .await?;
        assert_eq!(rows.try_next().await?.unwrap().as_slice(), &[1]);
        // The yielded batch owns its values, releasing the copied leaf's guard.
        drop(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                first_leaf.write::<Node<Vec<Vec<i16>>>>(),
            )
            .await
            .expect("projected batch retained its leaf read guard")?,
        );
        assert!(tree.try_write().is_err());
        drop(rows);
        drop(tree.try_write()?);

        assert_eq!(
            tree.read()
                .await
                .select(Range::<i16>::default(), vec![3], false)
                .await
                .err()
                .expect("invalid selected column")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(
            tree.read()
                .await
                .select(Range::from(smallvec![50i16]), vec![0], false)
                .await?
                .try_next()
                .await?
                .is_none()
        );

        tree.write().await.truncate().await?;
        tree.write().await.insert(vec![1, 2, 3]).await?;
        {
            let contents = dir.read().await;
            let mut root = contents
                .get_file(
                    uuid::Uuid::nil()
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer()),
                )
                .unwrap()
                .write::<Node<Vec<Vec<i16>>>>()
                .await?;
            let Node::Leaf(rows) = &mut *root else {
                panic!("expected root leaf")
            };
            rows[0].pop();
        }
        let error = match tree
            .read()
            .await
            .select(Range::<i16>::default(), vec![0], false)
            .await
        {
            Ok(mut rows) => rows.try_next().await.unwrap_err(),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        for reverse in [false, true] {
            let error = match tree
                .read()
                .await
                .select(Range::<i16>::default(), vec![0, 1, 2], reverse)
                .await
            {
                Ok(mut rows) => rows.try_next().await.unwrap_err(),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "stored row does not match the BTree schema"
            );
            drop(tree.try_write()?);
        }
    }

    fixture.close()
}

#[tokio::test]
async fn borrowed_visits_preserve_order_and_release_guards() -> io::Result<()> {
    use std::ops::{Bound, ControlFlow};
    use std::sync::Arc;

    fn send<T: Send>(value: T) -> T {
        value
    }

    let fixture = TestDir::new("test_btree_borrowed_visit")?;
    {
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
        let dir = cache.load(fixture.path().to_owned())?;
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(2),
            Collator::default(),
            dir.clone(),
        )
        .await?;
        for key in 0..20 {
            tree.write().await.insert(vec![key, key + 100]).await?;
        }

        let reader = tree.read().await;
        for reverse in [false, true] {
            let start = 3;
            let end = 17;
            let range = Range::with_bounds(
                smallvec![],
                (Bound::Included(&start), Bound::Excluded(&end)),
            );
            let mut actual = Vec::new();
            send(reader.visit(range, reverse, |row| {
                actual.push((row[0], row[1]));
                Ok::<_, io::Error>(ControlFlow::Continue(()))
            }))
            .await?;
            let mut expected: Vec<_> = (start..end).map(|key| (key, key + 100)).collect();
            if reverse {
                expected.reverse();
            }
            assert_eq!(actual, expected);
        }

        let (first_leaf, address) = {
            let contents = dir.read().await;
            let mut id = uuid::Uuid::nil();
            loop {
                let file = contents
                    .get_file(
                        id.hyphenated()
                            .encode_lower(&mut uuid::Uuid::encode_buffer()),
                    )
                    .unwrap();
                let node = file.read::<Node<Vec<Vec<i16>>>>().await?;
                match &*node {
                    Node::Leaf(rows) => break (file.clone(), rows[0].as_ptr() as usize),
                    Node::Index(_, children) => id = children[0],
                }
            }
        };
        let mut visited = 0;
        reader
            .visit(Range::<i16>::default(), false, |row| {
                // The callback receives the stored row, not a copied batch.
                assert_eq!(row.as_ptr() as usize, address);
                visited += 1;
                Ok::<_, io::Error>(ControlFlow::Break(()))
            })
            .await?;
        assert_eq!(visited, 1);
        drop(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                first_leaf.write::<Node<Vec<Vec<i16>>>>(),
            )
            .await
            .expect("early stop retained a leaf guard")?,
        );

        let error = reader
            .visit(Range::<i16>::default(), false, |_| {
                Err::<ControlFlow<()>, _>(io::Error::other("visitor failure"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "visitor failure");
        assert!(!freqfs::IntegrityError::is_marked(&error));
        drop(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                first_leaf.write::<Node<Vec<Vec<i16>>>>(),
            )
            .await
            .expect("callback error retained a leaf guard")?,
        );

        let marker = Arc::new(());
        let weak = Arc::downgrade(&marker);
        let unpolled = reader.visit(Range::<i16>::default(), false, move |_| {
            let _retained = &marker;
            Ok::<_, io::Error>(ControlFlow::Continue(()))
        });
        assert!(weak.upgrade().is_some());
        drop(unpolled);
        assert!(weak.upgrade().is_none());

        {
            let held = first_leaf.write::<Node<Vec<Vec<i16>>>>().await?;
            let marker = Arc::new(());
            let weak = Arc::downgrade(&marker);
            {
                let visit = reader.visit(
                    Range::<i16>::default(),
                    false,
                    move |_| -> io::Result<ControlFlow<()>> {
                        let _retained = &marker;
                        panic!("blocked scan invoked its callback");
                    },
                );
                futures::pin_mut!(visit);
                assert!(futures::poll!(&mut visit).is_pending());
                assert!(weak.upgrade().is_some());
            }
            assert!(weak.upgrade().is_none());
            drop(held);
        }
        drop(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                first_leaf.write::<Node<Vec<Vec<i16>>>>(),
            )
            .await
            .expect("cancelled scan retained a leaf guard")?,
        );

        let mut missing = 0;
        reader
            .visit(Range::from(smallvec![50i16]), false, |_| {
                missing += 1;
                Ok::<_, io::Error>(ControlFlow::Continue(()))
            })
            .await?;
        assert_eq!(missing, 0);

        let reversed = Range::with_bounds(
            Vec::new(),
            (
                std::ops::Bound::Included(5i16),
                std::ops::Bound::Excluded(1),
            ),
        );
        assert_eq!(
            reader.count(&reversed).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        for bounds in [
            (
                std::ops::Bound::Included(1i16),
                std::ops::Bound::Excluded(1),
            ),
            (
                std::ops::Bound::Excluded(1i16),
                std::ops::Bound::Excluded(1),
            ),
        ] {
            assert_eq!(
                reader
                    .count(&Range::with_bounds(Vec::new(), bounds))
                    .await?,
                0
            );
        }
        assert_eq!(
            reader
                .visit(
                    Range::from(smallvec![1i16, 2, 3]),
                    false,
                    |_| -> io::Result<ControlFlow<()>> {
                        panic!("invalid range invoked its callback");
                    }
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        drop(reader);
        drop(tree.try_write()?);
    }
    fixture.close()
}

#[tokio::test]
async fn count_releases_parent_access_with_one_io_slot() -> io::Result<()> {
    let fixture = TestDir::new("count_one_slot")?;
    {
        let dir = Cache::<File>::new(
            1024 * BLOCK_SIZE,
            None,
            0,
            std::time::Duration::from_secs(3),
        )
        .load(fixture.path().to_owned())?;
        let tree =
            BTreeLock::create(ExampleSchema::<i16>::new(1), Collator::default(), dir).await?;
        for key in 0..40 {
            tree.write().await.insert(vec![key]).await?;
        }
        tree.sync_all().await?;
    }
    {
        let dir = Cache::<File>::new(
            1024 * BLOCK_SIZE,
            Some(1),
            0,
            std::time::Duration::from_secs(1),
        )
        .load(fixture.path().to_owned())?;
        let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Collator::default(), dir)?;
        assert_eq!(tree.read().await.count(&Range::<i16>::default()).await?, 40);
    }
    fixture.close()
}

#[test]
fn ordinary_traversals_reject_ancestor_cycles() -> io::Result<()> {
    // A hot malformed traversal need not yield to a Tokio timer. Bound the whole
    // fixture externally as well, so a regression cannot hang the native suite.
    const CHILD: &str = "BTREE_CYCLE_REGRESSION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let mut child = std::process::Command::new(std::env::current_exe()?)
            .args([
                "lifecycle::ordinary_traversals_reject_ancestor_cycles",
                "--exact",
            ])
            .env(CHILD, "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if child.try_wait()?.is_some() {
                let output = child.wait_with_output()?;
                let stdout = String::from_utf8_lossy(&output.stdout);
                assert!(
                    output.status.success(),
                    "{stdout}\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored"));
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                child.kill()?;
                child.wait()?;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "corrupt traversal did not terminate",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let fixture = TestDir::new("tree_cycle")?;
            {
                let dir = Cache::<File>::new(
                    1024 * BLOCK_SIZE,
                    Some(1),
                    0,
                    std::time::Duration::from_secs(1),
                )
                .load(fixture.path().to_owned())?;
                let tree = BTreeLock::create(
                    ExampleSchema::<i16>::new(1),
                    Collator::default(),
                    dir.clone(),
                )
                .await?;
                let root = uuid::Uuid::nil();
                let ids = [1, 2].map(uuid::Uuid::from_u128);
                let nodes = [
                    Node::Index(vec![vec![1]], vec![root, ids[1]]),
                    Node::Index(vec![vec![1]], vec![root, ids[0]]),
                ];
                {
                    let mut native = dir.write().await;
                    for (id, node) in ids.into_iter().zip(nodes) {
                        native.create_file(id.to_string(), node).await?;
                    }
                }
                // Retain the original repeated-edge case and a real ancestry cycle
                // with distinct child IDs in every local node.
                for children in [vec![root, root], ids.to_vec()] {
                    *dir.read()
                        .await
                        .write_file::<Node<Vec<Vec<i16>>>>(&root.to_string())
                        .await? = Node::Index(vec![vec![1]], children.clone());
                    {
                        let reader = tree.read().await;
                        for operation in [
                            "contains", "first", "last", "count", "scan", "reverse", "visit",
                        ] {
                            let error =
                                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                                    match operation {
                                        "contains" => reader.contains(&[1]).await.map(|_| ()),
                                        "first" => {
                                            reader.first(Range::<i16>::default()).await.map(|_| ())
                                        }
                                        "last" => {
                                            reader.last(Range::<i16>::default()).await.map(|_| ())
                                        }
                                        "count" => {
                                            reader.count(&Range::<i16>::default()).await.map(|_| ())
                                        }
                                        "scan" => reader
                                            .clone()
                                            .keys(Range::<i16>::default())
                                            .await?
                                            .try_next()
                                            .await
                                            .map(|_| ()),
                                        "reverse" => reader
                                            .clone()
                                            .keys_rev(Range::<i16>::default())
                                            .await?
                                            .try_next()
                                            .await
                                            .map(|_| ()),
                                        _ => {
                                            reader
                                                .visit(Range::<i16>::default(), false, |_| {
                                                    Ok::<_, io::Error>(
                                                        std::ops::ControlFlow::Continue(()),
                                                    )
                                                })
                                                .await
                                        }
                                    }
                                })
                                .await
                                .expect("corrupt traversal must terminate")
                                .unwrap_err();
                            assert!(freqfs::IntegrityError::is_marked(&error));
                        }
                    }
                    for empty in [false, true] {
                        let values = if empty { vec![] } else { vec![Ok(vec![2])] };
                        let error = tree
                            .write()
                            .await
                            .insert_sorted(futures::stream::iter(values))
                            .await
                            .unwrap_err();
                        assert!(freqfs::IntegrityError::is_marked(&error));
                        drop(tree.try_write()?);
                    }
                    let node = dir
                        .read()
                        .await
                        .read_file::<Node<Vec<Vec<i16>>>>(&root.to_string())
                        .await?
                        .clone();
                    let Node::Index(separators, actual) = node else {
                        panic!("root changed kind")
                    };
                    assert_eq!(separators, vec![vec![1]]);
                    assert_eq!(actual, children);
                }
            }
            fixture.close()
        })
}

#[tokio::test]
async fn borrowed_exact_lookup_checks_uniqueness_before_callback() -> io::Result<()> {
    #[derive(Clone, Eq, PartialEq)]
    struct Equal;

    impl collate::Collate for Equal {
        type Value = i16;

        fn cmp(&self, _: &i16, _: &i16) -> std::cmp::Ordering {
            std::cmp::Ordering::Equal
        }
    }

    fn send<T: Send>(value: T) -> T {
        value
    }

    let fixture = TestDir::new("test_btree_borrowed_lookup")?;
    {
        let dir = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3))
            .load(fixture.path().to_owned())?;
        let mut schema = ExampleSchema::<i16>::new(2);
        schema.order = 3;
        let tree = BTreeLock::create(schema, Equal, dir.clone()).await?;
        for row in [[0, 0], [1, 10], [1, 11], [2, 20], [3, 30]] {
            tree.write().await.insert(row.to_vec()).await?;
        }

        let reader = tree.read().await;
        let expected = vec![1, 10];
        let mut calls = 0;
        let result = send(reader.get_with(&[1, 10], |row| {
            calls += 1;
            assert_eq!(row, expected);
            Ok::<_, io::Error>(&expected)
        }))
        .await?;
        assert_eq!(result, Some(&expected));
        assert_eq!(calls, 1);

        for prefix in [vec![1], vec![9]] {
            let mut called = false;
            let result = reader
                .get_with(&prefix, |_| {
                    called = true;
                    Err::<(), _>(io::Error::other("callback must not run"))
                })
                .await;
            assert!(!called);
            if prefix == [1] {
                let error = result.unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                assert_eq!(error.to_string(), "ambiguous semantic key");
                assert!(!freqfs::IntegrityError::is_marked(&error));
            } else {
                assert_eq!(result?, None);
            }
        }

        LEAF_READS
            .scope(std::cell::Cell::new(0), async {
                assert_eq!(reader.get(&[1, 10]).await?.unwrap().as_slice(), &[1, 10]);
                let owned_reads = LEAF_READS.with(std::cell::Cell::get);
                LEAF_READS.with(|reads| reads.set(0));
                assert_eq!(
                    reader
                        .get_with(&[1, 10], |row| Ok::<_, io::Error>(row[1]))
                        .await?,
                    Some(10)
                );
                assert_eq!(LEAF_READS.with(std::cell::Cell::get), owned_reads + 1);
                Ok::<_, io::Error>(())
            })
            .await?;

        let error = reader
            .get_with(&[1, 10], |_| Err::<(), _>(io::Error::other("read failure")))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "read failure");
        assert!(!freqfs::IntegrityError::is_marked(&error));
        drop(reader);
        drop(tree.try_write()?);

        tree.write().await.truncate().await?;
        tree.write().await.insert(vec![5, 50]).await?;
        {
            let contents = dir.read().await;
            let mut root = contents
                .get_file(
                    uuid::Uuid::nil()
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer()),
                )
                .unwrap()
                .write::<Node<Vec<Vec<i16>>>>()
                .await?;
            let Node::Leaf(rows) = &mut *root else {
                panic!("expected root leaf")
            };
            rows[0].pop();
        }
        let reader = tree.read().await;
        let mut called = false;
        assert_eq!(
            reader
                .get_with(&[5], |row| {
                    called = true;
                    assert_eq!(row, &[5]);
                    Err::<(), _>(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "consumer rejected malformed row",
                    ))
                })
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(
            !called,
            "exact lookup rejects malformed width before the callback"
        );
        called = false;
        assert_eq!(
            reader
                .visit(Range::<i16>::default(), false, |row| {
                    called = true;
                    assert_eq!(row, &[5]);
                    Err::<std::ops::ControlFlow<()>, _>(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "consumer rejected malformed row",
                    ))
                })
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(!called, "visit rejects malformed width before the callback");
    }
    fixture.close()
}

#[tokio::test]
async fn ordinary_reads_reject_malformed_index_separators() -> io::Result<()> {
    // Borrowed schema metadata must remain usable by scoped Send streams.
    #[derive(Debug, Eq, PartialEq)]
    struct CheckedSchema<'a>(&'a usize);

    impl Schema for CheckedSchema<'_> {
        type Error = io::Error;
        type Value = i16;

        fn block_size(&self) -> usize {
            BLOCK_SIZE
        }

        fn len(&self) -> usize {
            *self.0
        }

        fn order(&self) -> usize {
            5
        }

        fn validate_prefix(&self, prefix: &[i16]) -> io::Result<()> {
            if prefix.is_empty() || prefix.len() > self.len() || prefix.iter().any(|v| *v < 0) {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid schema prefix",
                ))
            } else {
                Ok(())
            }
        }

        fn validate_key(&self, key: Vec<i16>) -> io::Result<Vec<i16>> {
            self.validate_prefix(&key)?;
            if key.len() != self.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid row width",
                ));
            }

            Ok(key)
        }
    }

    fn send<T: Send>(value: T) -> T {
        value
    }

    let fixture = TestDir::new("test_btree_index_validation")?;
    {
        let width = 2;
        let dir = Cache::<File>::new(1024 * 1024, Some(1), 0, std::time::Duration::from_secs(3))
            .load(fixture.path().to_owned())?;
        let tree =
            BTreeLock::create(CheckedSchema(&width), Collator::default(), dir.clone()).await?;
        let ids = [1, 2, 3].map(uuid::Uuid::from_u128);
        let root_id = uuid::Uuid::nil();
        let root;
        let left;
        {
            let mut native = dir.write().await;
            for (id, value) in ids.into_iter().zip([10, 20, 30]) {
                native
                    .create_file(
                        id.hyphenated()
                            .encode_lower(&mut uuid::Uuid::encode_buffer())
                            .to_owned(),
                        Node::Leaf(vec![vec![value, 1], vec![value + 1, 1]]),
                    )
                    .await?;
            }
            root = native
                .get_file(
                    root_id
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer()),
                )
                .unwrap()
                .clone();
            left = native
                .get_file(
                    ids[0]
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer()),
                )
                .unwrap()
                .clone();
            *root.write::<Node<Vec<Vec<i16>>>>().await? =
                Node::Index(vec![vec![20], vec![30]], ids.to_vec());
        }
        tree.validate().await?;
        assert!(tree.read().await.contains(&[10, 1]).await?);
        {
            let stream = send(
                tree.clone()
                    .into_read()
                    .await
                    .keys(Range::<i16>::default())
                    .await?,
            );
            assert_eq!(stream.try_collect::<Vec<_>>().await?.len(), 6);
            let reader = tree.read().await;
            let mut values = Vec::new();
            send(reader.visit(Range::<i16>::default(), false, |row| {
                values.push(row[0]);
                Ok::<_, io::Error>(std::ops::ControlFlow::Continue(()))
            }))
            .await?;
            assert_eq!(values, [10, 11, 20, 21, 30, 31]);
        }
        let root_value = root.read::<Node<Vec<Vec<i16>>>>().await?.clone();
        let left_value = left.read::<Node<Vec<Vec<i16>>>>().await?.clone();
        for nested in [false, true] {
            for (case, (separators, children)) in [
                (vec![vec![20]], ids.to_vec()),
                (vec![vec![], vec![30]], ids.to_vec()),
                (vec![vec![20, 1, 2], vec![30]], ids.to_vec()),
                (vec![vec![-1], vec![30]], ids.to_vec()),
                (vec![vec![30], vec![20]], ids.to_vec()),
                (vec![vec![20], vec![30]], vec![ids[1], ids[1], ids[2]]),
                (vec![vec![20], vec![30]], vec![ids[1], ids[2], ids[1]]),
            ]
            .into_iter()
            .enumerate()
            {
                let target = if nested { &left } else { &root };
                *target.write::<Node<Vec<Vec<i16>>>>().await? =
                    Node::Index(separators.clone(), children.clone());
                let check = |error: io::Error| {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    assert!(freqfs::IntegrityError::is_marked(&error));
                    if (1..=3).contains(&case) {
                        // The schema's original cause survives the storage marker.
                        let source = std::error::Error::source(error.get_ref().unwrap())
                            .unwrap()
                            .downcast_ref::<io::Error>()
                            .unwrap();
                        assert_eq!(
                            source
                                .get_ref()
                                .unwrap()
                                .downcast_ref::<io::Error>()
                                .unwrap()
                                .kind(),
                            io::ErrorKind::InvalidInput
                        );
                    }
                };
                let range = Range::from_prefix(vec![10]);
                {
                    let reader = tree.read().await;
                    check(reader.contains(&[10, 1]).await.unwrap_err());
                    check(reader.get(&[10]).await.unwrap_err());
                    check(
                        reader
                            .get_with::<(), io::Error>(&[10], |_| {
                                panic!("malformed index reached callback")
                            })
                            .await
                            .unwrap_err(),
                    );
                    check(reader.count(&range).await.unwrap_err());
                    check(reader.first(range.clone()).await.unwrap_err());
                    check(reader.last(range.clone()).await.unwrap_err());
                    check(reader.is_empty(range.clone()).await.unwrap_err());
                    for reverse in [false, true] {
                        let mut called = false;
                        let error = reader
                            .visit(range.clone(), reverse, |_| {
                                called = true;
                                Ok::<_, io::Error>(std::ops::ControlFlow::Continue(()))
                            })
                            .await
                            .unwrap_err();
                        check(error);
                        assert!(!called);
                    }
                }
                for reverse in [false, true] {
                    for projected in [false, true] {
                        let reader = tree.clone().into_read().await;
                        let result = if projected {
                            reader.select(range.clone(), vec![0], reverse).await
                        } else if reverse {
                            reader.keys_rev(range.clone()).await
                        } else {
                            reader.keys(range.clone()).await
                        };
                        match result {
                            Ok(mut rows) => check(rows.try_next().await.unwrap_err()),
                            Err(error) => check(error),
                        }
                        drop(tree.try_write()?);
                    }
                }
                if !nested {
                    check(
                        tree.write()
                            .await
                            .insert_sorted(futures::stream::iter([Ok(vec![40, 2])]))
                            .await
                            .unwrap_err(),
                    );
                }
                check(tree.write().await.insert(vec![10, 2]).await.unwrap_err());
                check(tree.write().await.delete(&[10, 1]).await.unwrap_err());
                check(
                    tree.write()
                        .await
                        .replace(&[10, 1], vec![10, 2])
                        .await
                        .unwrap_err(),
                );
                {
                    let unchanged = target.read::<Node<Vec<Vec<i16>>>>().await?;
                    let Node::Index(actual, actual_children) = &*unchanged else {
                        panic!("stored node was replaced")
                    };
                    assert_eq!(actual, &separators);
                    assert_eq!(actual_children, &children);
                }
                drop(tree.try_write()?);
                *target.write::<Node<Vec<Vec<i16>>>>().await? = if nested {
                    left_value.clone()
                } else {
                    root_value.clone()
                };
            }
        }
        tree.validate().await?;
        assert_eq!(tree.read().await.count(&Range::<i16>::default()).await?, 6);
    }
    fixture.close()
}
