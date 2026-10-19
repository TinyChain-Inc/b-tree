use super::*;

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
                    for replacement in [10, 20, count * 10] {
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
