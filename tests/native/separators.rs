use super::*;

#[tokio::test]
async fn in_place_splits_merges_copy_and_reopening() -> Result<(), io::Error> {
    for order in [3, 4, 5] {
        let fixture = TestDir::new("test_btree")?;
        let path = fixture.path().to_owned();

        {
            let mut schema = ExampleSchema::<i16>::new(1);
            schema.order = order;
            let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
            let dir = cache.clone().load(path.clone())?;
            let tree = BTreeLock::create(schema, Collator::default(), dir.clone()).await?;
            for key in 0..100 {
                tree.write().await.insert(vec![key]).await?;
            }
            let files = dir.read().await.len();
            for _ in 0..20 {
                tree.write().await.insert(vec![99]).await?;
            }
            assert_eq!(dir.read().await.len(), files);
            for key in 0..90 {
                tree.write().await.delete(&[key]).await?;
            }
            for key in 100..150 {
                tree.write().await.insert(vec![key]).await?;
            }
            tree.validate().await?;
            tree.sync_all().await?;
            drop(tree);
            let reopened =
                Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3))
                    .load(path.clone())?;
            let mut schema = ExampleSchema::<i16>::new(1);
            schema.order = order;
            let tree = BTreeLock::load(schema, Collator::default(), reopened.clone())?;
            tree.validate().await?;
            let keys: Vec<_> = tree
                .read()
                .await
                .keys(Range::<i16>::default())
                .await?
                .map_ok(|key| key[0])
                .try_collect()
                .await?;
            assert_eq!(keys, (90..150).collect::<Vec<_>>());
            let copy_fixture = if order == 4 {
                Some(TestDir::new("test_btree_copy")?)
            } else {
                None
            };
            let copied = if let Some(fixture) = &copy_fixture {
                let copy_path = fixture.path().to_owned();
                let copy_dir = cache.load(copy_path.clone())?;
                let copy = tree.copy_into(copy_dir.clone()).await?;
                assert!(matches!(
                    tree.copy_into(copy_dir.clone()).await,
                    Err(err) if err.kind() == io::ErrorKind::AlreadyExists
                ));
                {
                    let source = reopened.read().await;
                    let target = copy_dir.read().await;
                    assert_eq!(source.len(), target.len());
                    for name in source.names() {
                        let source = source.read_file::<_, Node<Vec<Vec<i16>>>>(name).await?;
                        let target = target.read_file::<_, Node<Vec<Vec<i16>>>>(name).await?;
                        match (&*source, &*target) {
                            (Node::Leaf(left), Node::Leaf(right)) => assert_eq!(left, right),
                            (
                                Node::Index(left, left_children),
                                Node::Index(right, right_children),
                            ) => {
                                assert_eq!(left, right);
                                assert_eq!(left_children, right_children);
                            }
                            _ => panic!("copy changed the node kind"),
                        }
                    }
                }
                Some((copy_dir, copy))
            } else {
                None
            };

            // Exercise the opposite redistribution direction and collapse naturally.
            for key in (90..150).rev() {
                assert!(tree.write().await.delete(&[key]).await?);
                tree.validate().await?;
            }
            assert_eq!(reopened.read().await.len(), 1);
            assert!(tree.read().await.is_empty(Range::<i16>::default()).await?);
            tree.write().await.truncate().await?;
            if let Some((copy_dir, copy)) = copied {
                assert_eq!(copy.read().await.count(&Range::<i16>::default()).await?, 60);
                let root = uuid::Uuid::nil();
                {
                    let contents = copy_dir.write().await;
                    let malformed = Node::Index(vec![vec![90]], vec![root]);

                    *contents.write_file::<_, Node<Vec<Vec<i16>>>>(&root).await? = malformed;
                }
                assert!(copy.validate().await.is_err());
                reopened.write().await.create_dir("unexpected".into())?;
                copy_dir.write().await.truncate().await;
                assert!(matches!(
                    tree.copy_into(copy_dir.clone()).await,
                    Err(err) if err.kind() == io::ErrorKind::InvalidData
                ));
            }
            if let Some(fixture) = copy_fixture {
                fixture.close()?;
            }
        }

        fixture.close()?;
    }
    Ok(())
}

#[tokio::test]
async fn compact_separators_preserve_complete_rows_and_boundaries() -> io::Result<()> {
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let dir = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(3),
            Collator::default(),
            dir.clone(),
        )
        .await?;
        let rows: Vec<_> = (0..48).map(|i| vec![i / 3, i % 3, 100 + i]).collect();
        tree.write()
            .await
            .insert_sorted(futures::stream::iter(rows.iter().cloned().map(Ok)))
            .await?;
        tree.validate().await?;

        {
            use std::ops::Bound::{Included, Unbounded};

            let view = tree.read().await;
            assert_eq!(
                view.count(&Range::from_prefix(smallvec![0, 0, 100]))
                    .await?,
                1
            );
            for (name, range) in [
                (
                    "extra prefix column",
                    Range::from_prefix(smallvec![0, 0, 0, 0]),
                ),
                (
                    "bound after full-width prefix",
                    Range::with_bounds(smallvec![0, 0, 0], (Included(0), Unbounded)),
                ),
            ] {
                assert!(
                    matches!(view.count(&range).await, Err(error) if error.kind() == io::ErrorKind::InvalidInput),
                    "{name}: count"
                );
                assert!(
                    matches!(view.first(range.as_ref()).await, Err(error) if error.kind() == io::ErrorKind::InvalidInput),
                    "{name}: first"
                );
                assert!(
                    matches!(view.last(range.as_ref()).await, Err(error) if error.kind() == io::ErrorKind::InvalidInput),
                    "{name}: last"
                );
                assert!(
                    matches!(view.clone().keys(range.clone()).await, Err(error) if error.kind() == io::ErrorKind::InvalidInput),
                    "{name}: keys"
                );
                assert!(
                    matches!(view.clone().keys_rev(range.clone()).await, Err(error) if error.kind() == io::ErrorKind::InvalidInput),
                    "{name}: reverse keys"
                );
                assert!(
                    matches!(view.clone().groups(range, 1, false).await, Err(error) if error.kind() == io::ErrorKind::InvalidInput),
                    "{name}: groups"
                );
            }
        }

        let files: Vec<_> = dir.read().await.files().cloned().collect();
        let mut widths = [false; 2];
        for file in &files {
            let node = file.read::<Node<Vec<Vec<i16>>>>().await?;
            match &*node {
                Node::Leaf(rows) => assert!(rows.iter().all(|row| row.len() == 3)),
                Node::Index(separators, children) => {
                    assert_eq!(children.len(), separators.len() + 1);
                    for separator in separators {
                        // Distinct logical keys never retain their payload column.
                        assert!(separator.len() <= 2);
                        widths[separator.len() - 1] = true;
                    }
                }
            }
        }
        assert_eq!(widths, [true, true]);
        INDEX_WRITES
            .scope(std::cell::Cell::new(0), async {
                for row in &rows {
                    assert!(
                        tree.write()
                            .await
                            .replace(row, vec![row[0], row[1], row[2] + 1000])
                            .await?
                    );
                }
                assert_eq!(
                    INDEX_WRITES.with(std::cell::Cell::get),
                    0,
                    "payload changes preserve routing separators"
                );
                Ok::<_, io::Error>(())
            })
            .await?;
        tree.validate().await?;
        let expected: Vec<_> = rows
            .iter()
            .map(|row| vec![row[0], row[1], row[2] + 1000])
            .collect();
        let actual: Vec<Vec<i16>> = tree
            .read()
            .await
            .keys(Range::<i16>::default())
            .await?
            .map_ok(|row| row.into_vec())
            .try_collect()
            .await?;
        assert_eq!(actual, expected);

        let root = dir
            .read()
            .await
            .get_file(&uuid::Uuid::nil().to_string())
            .unwrap()
            .clone();
        let saved = (*root.read::<Node<Vec<Vec<i16>>>>().await?).clone();
        let Node::Index(separators, children) = &saved else {
            panic!("multilevel tree")
        };
        let invalid_prefixes = [
            ("empty prefix", vec![]),
            ("oversized prefix", vec![0, 0, 0, 0]),
            ("below left subtree", vec![-1]),
            ("above right subtree", vec![1000]),
        ]
        .map(|(name, prefix)| {
            let mut changed = separators.clone();
            changed[0] = prefix;
            (name, changed)
        });

        let mut extra_separator = separators.clone();
        extra_separator.insert(0, expected[0].clone());

        for (name, changed) in invalid_prefixes
            .into_iter()
            .chain([("wrong child count", extra_separator)])
        {
            *root.write::<Node<Vec<Vec<i16>>>>().await? = Node::Index(changed, children.clone());
            assert!(tree.validate().await.is_err(), "{name}");
        }

        // Finish structural validation before identity lookups: a tied leaf visited
        // first must not send a lookup through an unchecked cyclic sibling.
        let (cycle_id, cycle) = dir
            .write()
            .await
            .create_file_unique(Node::Leaf(Vec::<Vec<i16>>::new()))
            .await?;
        *cycle.write::<Node<Vec<Vec<i16>>>>().await? =
            Node::Index(vec![vec![0, 0, 0]; 2], vec![cycle_id; 3]);
        let (leaf_id, _) = dir
            .write()
            .await
            .create_file_unique(Node::Leaf(vec![vec![0, 0, 0]; 2]))
            .await?;
        *root.write::<Node<Vec<Vec<i16>>>>().await? =
            Node::Index(vec![vec![0, 0, 0]], vec![cycle_id, leaf_id]);
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), tree.validate())
            .await
            .expect("cyclic storage validation must terminate")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        *root.write::<Node<Vec<Vec<i16>>>>().await? = saved;
        drop(cycle);
        {
            let mut files = dir.write().await;
            assert!(files.delete(&cycle_id).await);
            assert!(files.delete(&leaf_id).await);
        }
        tree.validate().await?;
        tree.sync_all().await?;
        drop(root);
        drop(files);
        drop(tree);
        drop(dir);
        let dir = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let reopened = BTreeLock::load(ExampleSchema::<i16>::new(3), Collator::default(), dir)?;
        reopened.validate().await?;
        let actual: Vec<Vec<i16>> = reopened
            .read()
            .await
            .keys_rev(Range::<i16>::default())
            .await?
            .map_ok(|row| row.into_vec())
            .try_collect()
            .await?;
        assert_eq!(actual, expected.into_iter().rev().collect::<Vec<_>>());
        drop(reopened);
    }

    fixture.close()
}

#[tokio::test]
async fn lookup_between_separators_preserves_range_results() -> Result<(), io::Error> {
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(1),
            Collator::<i16>::default(),
            cache.load(path.clone())?,
        )
        .await?;
        for key in (0..100).step_by(2) {
            tree.write().await.insert(vec![key]).await?;
        }
        {
            let view = tree.read().await;
            for key in 0..100 {
                let first = view
                    .first(Range::from(
                        vec![key].into_iter().collect::<b_tree::Key<i16>>(),
                    ))
                    .await?;
                assert_eq!(first.map(|row| row[0]), (key % 2 == 0).then_some(key));
            }
        }
    }

    fixture.close()
}

#[tokio::test]
async fn load_requires_existing_root() -> Result<(), io::Error> {
    let fixture = TestDir::new("test_btree")?;
    let path = fixture.path().to_owned();

    {
        let cache = Cache::<File>::new(BLOCK_SIZE, None, 0, std::time::Duration::from_secs(3));
        let dir = cache.load(path)?;
        assert!(
            BTreeLock::load(
                ExampleSchema::<i16>::new(1),
                Collator::<i16>::default(),
                dir.clone()
            )
            .is_err()
        );
        assert!(dir.read().await.is_empty());
        for order in [0, 1, 2] {
            let mut schema = ExampleSchema::<i16>::new(1);
            schema.order = order;
            assert!(matches!(
                BTreeLock::create(schema, Collator::<i16>::default(), dir.clone()).await,
                Err(error) if error.kind() == io::ErrorKind::InvalidInput
            ));
            assert!(
                dir.read().await.is_empty(),
                "order {order} must not create a root"
            );
        }

        let tree = BTreeLock::create(
            ExampleSchema::<i16>::new(1),
            Collator::<i16>::default(),
            dir.clone(),
        )
        .await?;
        tree.sync().await?;
        for order in [0, 1, 2] {
            let mut schema = ExampleSchema::<i16>::new(1);
            schema.order = order;
            assert!(matches!(
                BTreeLock::load(schema, Collator::<i16>::default(), dir.clone()),
                Err(error) if error.kind() == io::ErrorKind::InvalidInput
            ));
        }
        assert!(
            BTreeLock::load(
                ExampleSchema::<i16>::new(1),
                Collator::<i16>::default(),
                dir.clone()
            )
            .is_ok()
        );
        assert!(
            BTreeLock::create(
                ExampleSchema::<i16>::new(1),
                Collator::<i16>::default(),
                dir
            )
            .await
            .is_err()
        );
    }

    fixture.close()
}
