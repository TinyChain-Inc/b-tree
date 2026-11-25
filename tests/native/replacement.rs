use super::*;

#[tokio::test]
async fn exact_keys_use_ordered_search_and_validation() -> io::Result<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct Counted(Arc<AtomicUsize>);

    impl PartialEq for Counted {
        fn eq(&self, other: &Self) -> bool {
            Arc::ptr_eq(&self.0, &other.0)
        }
    }

    impl Eq for Counted {}

    impl collate::Collate for Counted {
        type Value = i16;

        fn cmp(&self, left: &i16, right: &i16) -> std::cmp::Ordering {
            self.0.fetch_add(1, Ordering::Relaxed);
            left.cmp(right)
        }
    }

    let fixture = TestDir::new("exact_ordered_search")?;
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let tree = BTreeLock::create(
        ExampleSchema::<i16>::new(1),
        Counted(count.clone()),
        cache.load(fixture.path().to_owned())?,
    )
    .await?;

    tree.write()
        .await
        .insert_sorted(futures::stream::iter((0..1024).map(|key| Ok(vec![key]))))
        .await?;

    count.store(0, Ordering::Relaxed);
    for key in [0, 511, 1023, 1024].into_iter().cycle().take(32) {
        assert_eq!(tree.read().await.get(&[key]).await?.is_some(), key < 1024);
    }
    assert!(count.swap(0, Ordering::Relaxed) < 32 * 256);
    tree.validate().await?;
    assert!(count.load(Ordering::Relaxed) < 1024 * 128);
    assert_eq!(
        tree.read().await.count(&Range::<i16>::default()).await?,
        1024
    );

    Ok(())
}

#[tokio::test]
async fn residual_ties_still_detect_nonadjacent_duplicates() -> io::Result<()> {
    #[derive(Clone, Eq, PartialEq)]
    struct Residual(bool);

    impl collate::Collate for Residual {
        type Value = i16;

        fn cmp(&self, left: &i16, right: &i16) -> std::cmp::Ordering {
            if self.0 {
                (left % 2).cmp(&(right % 2))
            } else {
                std::cmp::Ordering::Equal
            }
        }
    }

    for parity in [false, true] {
        let fixture = TestDir::new("residual_duplicates")?;
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
        let dir = cache.load(fixture.path().to_owned())?;
        let tree =
            BTreeLock::create(ExampleSchema::<i16>::new(1), Residual(parity), dir.clone()).await?;
        for key in [1, 3, 5] {
            tree.write().await.insert(vec![key]).await?;
        }
        for key in [1, 3, 5] {
            assert_eq!(
                tree.read().await.get(&[key]).await?.unwrap().as_slice(),
                &[key]
            );
        }
        let root = dir
            .read()
            .await
            .get_file(&uuid::Uuid::nil().to_string())
            .unwrap()
            .clone();
        *root.write::<Node<Vec<Vec<i16>>>>().await? = Node::Leaf(vec![vec![1], vec![3], vec![1]]);
        assert!(tree.read().await.get(&[1]).await.is_err());
        assert!(tree.validate().await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn replacement_relocates_across_leaves_and_reopens() -> io::Result<()> {
    let fixture = TestDir::new("relocating_replacement")?;
    {
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(1),
            Collator::default(),
            cache.load(fixture.path().to_owned())?,
        )
        .await?;

        for key in 0..80 {
            tree.write().await.insert(vec![key * 10]).await?;
        }
        assert!(tree.write().await.replace(&[0], vec![999]).await?);
        assert!(tree.write().await.replace(&[790], vec![-1]).await?);
        assert!(!tree.read().await.contains(&[0]).await?);
        assert!(tree.read().await.contains(&[999]).await?);
        tree.validate().await?;
        tree.sync().await?;
    }
    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let tree = BTreeLock::load(
        ExampleSchema::<i16>::new(1),
        Collator::default(),
        cache.load(fixture.path().to_owned())?,
    )?;

    tree.validate().await?;
    assert_eq!(
        tree.read()
            .await
            .keys::<i16>(Default::default())
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        80
    );

    Ok(())
}

#[tokio::test]
async fn deletion_requires_complete_keys_before_mutation() -> io::Result<()> {
    use futures::FutureExt;

    for count in [1_i16, 80] {
        let fixture = TestDir::new("complete_deletion_keys")?;
        {
            let cache =
                Cache::<File>::new(1024 * 1024, Some(1), 0, std::time::Duration::from_secs(3));
            let dir = cache.load(fixture.path().to_owned())?;

            let tree = BTreeLock::create(
                ExampleSchema::<i16>::new(2),
                Collator::default(),
                dir.clone(),
            )
            .await?;

            for key in 0..count {
                tree.write().await.insert(vec![key, key + 100]).await?;
            }

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
            {
                let mut writer = tree.write().await;
                let held = root.write::<Node<Vec<Vec<i16>>>>().await?;
                let before = held.clone();
                for key in [vec![], vec![0], vec![0, 100, 200]] {
                    let error = writer
                        .delete(&key)
                        .now_or_never()
                        .expect("invalid key waited for storage")
                        .unwrap_err();

                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                    let borrowed: Vec<_> = key.iter().collect();
                    let error = writer
                        .delete(&borrowed)
                        .now_or_never()
                        .expect("invalid borrowed key waited for storage")
                        .unwrap_err();

                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                }
                match (&*held, &before) {
                    (Node::Leaf(actual), Node::Leaf(expected)) => assert_eq!(actual, expected),
                    (Node::Index(actual, children), Node::Index(expected, expected_children)) => {
                        assert_eq!(actual, expected);
                        assert_eq!(children, expected_children);
                    }
                    _ => panic!("invalid deletion changed the root type"),
                }
            }

            let rows: Vec<_> = tree
                .read()
                .await
                .keys(Range::<i16>::default())
                .await?
                .map_ok(|row| row.into_vec())
                .try_collect()
                .await?;

            assert_eq!(
                rows,
                (0..count)
                    .map(|key| vec![key, key + 100])
                    .collect::<Vec<_>>()
            );

            {
                let mut writer = tree.write().await;
                assert!(!writer.delete(&[999, 999]).await?);
                let key = [0, 100];
                assert!(writer.delete(&[&key[0], &key[1]]).await?);
                assert!(!writer.delete(&key).await?);
                if count > 1 {
                    assert!(writer.delete(&[1, 101]).await?);
                }
            }
            tree.validate().await?;
            tree.sync_all().await?;
        }
        {
            let cache =
                Cache::<File>::new(1024 * 1024, Some(1), 0, std::time::Duration::from_secs(3));
            let tree = BTreeLock::load(
                ExampleSchema::<i16>::new(2),
                Collator::default(),
                cache.load(fixture.path().to_owned())?,
            )?;

            tree.validate().await?;
            let rows: Vec<_> = tree
                .read()
                .await
                .keys(Range::<i16>::default())
                .await?
                .map_ok(|row| row.into_vec())
                .try_collect()
                .await?;

            let start = if count == 1 { 1 } else { 2 };
            assert_eq!(
                rows,
                (start..count)
                    .map(|key| vec![key, key + 100])
                    .collect::<Vec<_>>()
            );
        }
        fixture.close()?;
    }
    Ok(())
}

#[tokio::test]
async fn replacement_preserves_slots_separators_and_topology() -> io::Result<()> {
    #[derive(Clone, Eq, PartialEq)]
    struct Tens;

    impl collate::Collate for Tens {
        type Value = i16;

        fn cmp(&self, left: &i16, right: &i16) -> std::cmp::Ordering {
            (left / 10).cmp(&(right / 10))
        }
    }

    for count in [1, 80] {
        let fixture = TestDir::new("test_btree")?;
        let path = fixture.path().to_owned();

        {
            let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1));
            let dir = cache.load(path.clone())?;
            let tree = BTreeLock::create(ExampleSchema::<i16>::new(1), Tens, dir.clone()).await?;
            assert!(!tree.write().await.replace(&[0], vec![1]).await?);
            tree.write()
                .await
                .insert_sorted(futures::stream::iter((0..count).map(|i| Ok(vec![10 * i]))))
                .await?;

            // Record physical nodes, all slots, and child identities, not just output.
            let files: Vec<_> = dir.read().await.files().cloned().collect();
            let mut before = Vec::new();
            for file in &files {
                before.push((*file.read::<Node<Vec<Vec<i16>>>>().await?).clone());
            }
            {
                let mut write = tree.write().await;
                assert!(!write.replace(&[count * 10], vec![count * 10 + 1]).await?);
                assert!(!write.replace(&[1], vec![2]).await?);
                assert_eq!(
                    write.replace(&[], vec![1]).await.unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );

                assert_eq!(
                    write.replace(&[0], vec![]).await.unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );

                if count > 1 {
                    for replacement in (1..count).take(2).map(|i| i * 10) {
                        assert_eq!(
                            write
                                .replace(&[0], vec![replacement])
                                .await
                                .unwrap_err()
                                .kind(),
                            io::ErrorKind::InvalidInput
                        );
                    }
                }
                for i in 0..count {
                    assert!(write.replace(&[10 * i], vec![10 * i + 1]).await?);
                    assert!(write.replace(&[10 * i + 1], vec![10 * i + 1]).await?);
                }
            }
            assert_eq!(dir.read().await.len(), files.len());
            for (file, old) in files.iter().zip(&before) {
                let node = file.read::<Node<Vec<Vec<i16>>>>().await?;
                let (old_rows, rows) = match (old, &*node) {
                    (Node::Leaf(old), Node::Leaf(new)) => (old, new),
                    (Node::Index(old, old_children), Node::Index(new, new_children)) => {
                        assert_eq!(old_children, new_children);
                        assert_eq!(
                            old, new,
                            "collator-equivalent changes preserve valid separators"
                        );

                        continue;
                    }
                    _ => panic!("replacement changed node kind"),
                };

                assert_eq!(old_rows.len(), rows.len());
                for (old, new) in old_rows.iter().zip(rows) {
                    assert_eq!(new, &vec![old[0] + 1]);
                }
            }
            // Row capacity is retained memory even for fixed-width scalar values.
            let mut grown = Vec::with_capacity(128);
            grown.push(2);
            assert!(tree.write().await.replace(&[1], grown).await?);
            assert!(tree.write().await.replace(&[2], vec![1]).await?);
            tree.validate().await?;
            tree.sync_all().await?;
            drop(files);
            drop(tree);
            drop(dir);
            let reopened =
                Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1))
                    .load(path.clone())?;
            let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Tens, reopened)?;
            tree.validate().await?;
            let actual: Vec<_> = tree
                .read()
                .await
                .keys(Range::<i16>::default())
                .await?
                .map_ok(|row| row[0])
                .try_collect()
                .await?;

            assert_eq!(actual, (0..count).map(|i| i * 10 + 1).collect::<Vec<_>>());
            drop(tree);
        }

        fixture.close()?;
    }

    Ok(())
}

#[tokio::test]
async fn replacement_accounts_actual_retained_growth() -> io::Result<()> {
    use uuid::Uuid;

    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let node = Node::Leaf(vec![vec![0]]);
        let initial = node.get_size();
        let dir = Cache::<File>::new(initial + 2, None, 0, std::time::Duration::from_millis(30))
            .load(path.clone())?;
        dir.write()
            .await
            .create_file(Uuid::nil().to_string(), node)
            .await?;

        let tree = BTreeLock::load(
            ExampleSchema::<i16>::new(1),
            Collator::default(),
            dir.clone(),
        )?;

        {
            let mut write = tree.write().await;
            let mut grown = Vec::with_capacity(2);
            grown.push(1);
            assert!(write.replace(&[0], grown).await?);
            let mut grown = Vec::with_capacity(3);
            grown.push(2);
            assert!(write.replace(&[1], grown).await?);
            assert_eq!(
                write
                    .first(Range::<i16>::default())
                    .await?
                    .unwrap()
                    .as_slice(),
                &[2]
            );

            assert!(write.replace(&[2], vec![0]).await?);
        }
        tree.validate().await?;
        drop(tree);
        drop(dir);
    }

    fixture.close()
}

#[tokio::test]
async fn replacement_distinguishes_equivalent_rows_across_leaves() -> io::Result<()> {
    use uuid::Uuid;

    #[derive(Clone, Eq, PartialEq)]
    struct Equal;

    impl collate::Collate for Equal {
        type Value = i16;

        fn cmp(&self, _: &i16, _: &i16) -> std::cmp::Ordering {
            std::cmp::Ordering::Equal
        }
    }

    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let dir = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let children = vec![Uuid::from_u128(1), Uuid::from_u128(2)];
        let entries = [
            (Uuid::nil(), Node::Index(vec![vec![1]], children.clone())),
            (children[0], Node::Leaf(vec![vec![0]])),
            (children[1], Node::Leaf(vec![vec![1]])),
        ];

        for (id, node) in entries {
            dir.write().await.create_file(id.to_string(), node).await?;
        }
        let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Equal, dir.clone())?;
        for previous in [0, 1] {
            assert!(
                tree.write()
                    .await
                    .replace(&[previous], vec![previous])
                    .await?
            );
        }
        assert!(!tree.write().await.replace(&[2], vec![2]).await?);
        assert!(tree.write().await.replace(&[0], vec![2]).await?);
        assert_eq!(
            tree.write()
                .await
                .replace(&[1], vec![2])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let rows: Vec<_> = tree
            .read()
            .await
            .keys(Range::<i16>::default())
            .await?
            .map_ok(|row| row[0])
            .try_collect()
            .await?;

        assert_eq!(rows, [2, 1]);
        drop(tree);
        drop(dir);
    }

    fixture.close()
}

#[tokio::test]
async fn replacement_cancellation_releases_prepared_ancestors() -> io::Result<()> {
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let dir = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(1),
            Collator::default(),
            dir.clone(),
        )
        .await?;

        tree.write()
            .await
            .insert_sorted(futures::stream::iter((0..80).map(|i| Ok(vec![i * 10]))))
            .await?;

        let files: Vec<_> = dir.read().await.files().cloned().collect();
        let root = dir
            .read()
            .await
            .get_file(&uuid::Uuid::nil().to_string())
            .unwrap()
            .clone();
        let (boundary, mut left_id) = {
            let node = root.read::<Node<Vec<Vec<i16>>>>().await?;
            let Node::Index(separators, children) = &*node else {
                panic!("multilevel root")
            };

            (separators[0][0], children[0])
        };

        let previous = loop {
            let file = dir
                .read()
                .await
                .get_file(&left_id.to_string())
                .unwrap()
                .clone();
            let node = file.read_owned::<Node<Vec<Vec<i16>>>>().await?;
            match &*node {
                Node::Leaf(rows) => {
                    break rows.last().unwrap()[0];
                }
                Node::Index(_, children) => left_id = *children.last().unwrap(),
            }
        };

        // Move the first right-side row left of its old root separator while remaining
        // above its predecessor; its leaf publication waits after the root.
        let replacement = boundary - 1;
        assert!(previous < replacement);
        let mut pinned = None;
        for file in &files {
            let node = file.read_owned::<Node<Vec<Vec<i16>>>>().await?;
            if matches!(&*node, Node::Leaf(rows) if rows[0] == vec![boundary]) {
                pinned = Some(node);
                break;
            }
        }
        assert!(pinned.is_some());
        {
            let mut write = tree.write().await;
            {
                let previous = [boundary];
                let mut pending = std::pin::pin!(write.replace(&previous, vec![replacement]));
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    loop {
                        assert!(futures::poll!(pending.as_mut()).is_pending());
                        if root.try_read::<Node<Vec<Vec<i16>>>>().is_err() {
                            break;
                        }
                        // Tokio may exhaust its cooperative budget during lookup,
                        // before replacement has acquired any publication guard.
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("replacement reached the pinned leaf");
            }
            assert!(root.try_read::<Node<Vec<Vec<i16>>>>().is_ok());
        }
        drop(pinned);
        assert_eq!(
            tree.read()
                .await
                .first(Range::<i16>::default())
                .await?
                .unwrap()
                .as_slice(),
            &[0]
        );

        tree.validate().await?;
        assert!(
            tree.write()
                .await
                .replace(&[boundary], vec![replacement])
                .await?
        );

        drop(files);
        drop(root);
        drop(tree);
        drop(dir);
    }

    fixture.close()
}
