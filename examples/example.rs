use std::marker::PhantomData;
use std::path::PathBuf;
use std::{fmt, io};

use collate::Collator;
use destream::{de, en};
use freqfs::Cache;
use futures::{TryFutureExt, TryStreamExt};
#[cfg(test)]
use get_size::GetSize;
use rand::RngExt;
use safecast::as_type;
use smallvec::smallvec;
use tokio::fs;

use b_tree::{BTreeLock, Node, Range, Schema};

#[tokio::test]
async fn root_creation_reclaims_cached_nodes() -> io::Result<()> {
    let path = setup_tmp_dir().await?;
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
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn missing_root_returns_errors_without_repair() -> io::Result<()> {
    let path = setup_tmp_dir().await?;
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
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn interrupted_truncate_does_not_publish_a_missing_root() -> io::Result<()> {
    for cancel in [false, true] {
        let path = setup_tmp_dir().await?;
        let empty_size = File::Node(Node::Leaf(vec![])).get_size();
        let cache = Cache::<File>::new(empty_size, None, 0, std::time::Duration::from_millis(30));
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
    Ok(())
}

#[tokio::test]
async fn sorted_ingestion_matches_insertion_and_survives_errors() -> Result<(), io::Error> {
    use futures::{StreamExt, stream};
    let path = setup_tmp_dir().await?;
    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let dir = cache.load(path.clone())?;
    let tree = BTreeLock::create(ExampleSchema::<i16>::new(1), Collator::default(), dir).await?;
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
    let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Collator::default(), reopened)?;
    let actual: Vec<_> = tree
        .read()
        .await
        .keys(Range::<i16>::default())
        .await?
        .map_ok(|key| key[0])
        .try_collect()
        .await?;
    assert_eq!(actual, (0..2002).collect::<Vec<_>>());
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
    drop(tree);
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn duplicate_insertion_needs_no_growth_admission() -> io::Result<()> {
    use futures::stream;
    use uuid::Uuid;

    for count in [1_i16, 5, 8] {
        let path = setup_tmp_dir().await?;
        let entries = if count == 8 {
            // Three levels exercise both recursive descent and root forwarding.
            let mut entries = vec![
                (
                    Uuid::nil(),
                    Node::Index(
                        vec![vec![0], vec![4]],
                        vec![Uuid::from_u128(1), Uuid::from_u128(2)],
                    ),
                ),
                (
                    Uuid::from_u128(1),
                    Node::Index(
                        vec![vec![0], vec![2]],
                        vec![Uuid::from_u128(3), Uuid::from_u128(4)],
                    ),
                ),
                (
                    Uuid::from_u128(2),
                    Node::Index(
                        vec![vec![4], vec![6]],
                        vec![Uuid::from_u128(5), Uuid::from_u128(6)],
                    ),
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
            let bound = node.get_size();
            dir.write()
                .await
                .create_file(id.to_string(), node, bound)
                .await?;
        }
        let tree = BTreeLock::load(
            ExampleSchema::<i16>::new(1),
            Collator::default(),
            dir.clone(),
        )?;
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
            let mut duplicate = Vec::with_capacity(1_000_000);
            duplicate.push(count - 1);
            assert_eq!(write.insert_sorted(stream::iter([Ok(duplicate)])).await?, 0);
            for ordered in [false, true] {
                let mut growth = Vec::with_capacity(1_000_000);
                growth.push(count);
                let error = if ordered {
                    write
                        .insert_sorted(stream::iter([Ok(growth)]))
                        .await
                        .unwrap_err()
                } else {
                    write.insert(growth).await.unwrap_err()
                };
                assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
            }
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
        let reopened = Cache::<File>::new(1_000_000, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let tree = BTreeLock::load(ExampleSchema::<i16>::new(1), Collator::default(), reopened)?;
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
        fs::remove_dir_all(path).await?;
    }
    Ok(())
}

const BLOCK_SIZE: usize = 4_096;

#[tokio::test]
async fn in_place_splits_merges_copy_and_reopening() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;
    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let dir = cache.clone().load(path.clone())?;
    let tree = BTreeLock::create(
        ExampleSchema::<i16>::new(1),
        Collator::default(),
        dir.clone(),
    )
    .await?;
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
    let reopened = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3))
        .load(path.clone())?;
    let tree = BTreeLock::load(
        ExampleSchema::<i16>::new(1),
        Collator::default(),
        reopened.clone(),
    )?;
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
    let copy_path = setup_tmp_dir().await?;
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
                (Node::Index(left, left_children), Node::Index(right, right_children)) => {
                    assert_eq!(left, right);
                    assert_eq!(left_children, right_children);
                }
                _ => panic!("copy changed the node kind"),
            }
        }
    }
    tree.write().await.truncate().await?;
    assert_eq!(copy.read().await.count(&Range::<i16>::default()).await?, 60);
    let root = uuid::Uuid::nil();
    {
        let contents = copy_dir.write().await;
        let malformed = Node::Index(vec![vec![90]], vec![root]);
        let bound = std::mem::size_of::<File>() + malformed.get_size();
        *contents
            .write_file::<_, Node<Vec<Vec<i16>>>>(&root, bound)
            .await? = malformed;
    }
    assert!(copy.validate().await.is_err());
    reopened.write().await.create_dir("unexpected".into())?;
    copy_dir.write().await.truncate().await;
    assert!(matches!(
        tree.copy_into(copy_dir.clone()).await,
        Err(err) if err.kind() == io::ErrorKind::InvalidData
    ));
    fs::remove_dir_all(path).await?;
    fs::remove_dir_all(copy_path).await
}

#[tokio::test]
async fn lookup_between_index_bounds_uses_the_preceding_child() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;
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
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn load_requires_existing_root() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;
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
    let tree = BTreeLock::create(
        ExampleSchema::<i16>::new(1),
        Collator::<i16>::default(),
        dir.clone(),
    )
    .await?;
    tree.sync().await?;
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
    Ok(())
}

#[derive(Clone)]
enum File {
    Node(Node<Vec<Vec<i16>>>),
}

impl de::FromStream for File {
    type Context = ();

    async fn from_stream<D: de::Decoder>(cxt: (), decoder: &mut D) -> Result<Self, D::Error> {
        Node::from_stream(cxt, decoder).map_ok(Self::Node).await
    }
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Node(node) => node.to_stream(encoder),
        }
    }
}

as_type!(File, Node, Node<Vec<Vec<i16>>>);

#[derive(Debug)]
struct ExampleSchema<T> {
    size: usize,
    value: PhantomData<T>,
}

impl<T> ExampleSchema<T> {
    fn new(size: usize) -> Self {
        Self {
            size,
            value: PhantomData,
        }
    }
}

impl<T> PartialEq for ExampleSchema<T> {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size
    }
}

impl<T> Eq for ExampleSchema<T> {}

impl<T: fmt::Debug> Schema for ExampleSchema<T> {
    type Error = io::Error;
    type Value = i16;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn len(&self) -> usize {
        self.size
    }

    fn order(&self) -> usize {
        5
    }

    fn validate_key(&self, key: Vec<i16>) -> Result<Vec<i16>, io::Error> {
        if key.len() == self.size {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("key length should be {}", self.size),
            ))
        }
    }
}

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut rng = rand::rng();
    loop {
        let rand: u32 = rng.random();
        let path = PathBuf::from(format!("/tmp/test_btree_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;
            break Ok(path);
        }
    }
}

async fn functional_test() -> Result<(), io::Error> {
    // set up the test directory
    let path = setup_tmp_dir().await?;

    // construct the schema
    let schema = ExampleSchema::<i16>::new(3);

    // initialize the cache
    let cache = Cache::<File>::new(
        schema.block_size(),
        None,
        0,
        std::time::Duration::from_secs(3),
    );

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

    {
        let view = btree.read().await;

        #[cfg(debug_assertions)]
        assert!(view.clone().is_valid().await?);

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

    {
        let view = btree.try_read().expect("btree read");

        #[cfg(debug_assertions)]
        assert!(view.clone().is_valid().await?);

        let mut keys = view.keys(Range::<i16>::default()).await?;
        assert_eq!(keys.try_next().await?, None);
    }

    // clean up
    fs::remove_dir_all(path).await
}

async fn load_test() -> Result<(), io::Error> {
    let n = 100_000;

    // set up the test directory
    let path = setup_tmp_dir().await?;

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
    fs::remove_dir_all(path).await
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    functional_test().await?;
    load_test().await?;
    Ok(())
}

impl get_size::GetSize for File {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Node(node) => node.get_heap_size(),
        }
    }
}

impl freqfs::FileLoad for File {
    async fn load_size(
        _: &std::path::Path,
        _: &mut tokio::fs::File,
        metadata: &std::fs::Metadata,
    ) -> io::Result<usize> {
        // TBON sequences have no allocation-sized length header. Every retained
        // row/cell needs encoded input; cover geometric Vec growth and scalar
        // payload bytes before decoding without retaining any parsed payload.
        let encoded = usize::try_from(metadata.len()).map_err(io::Error::other)?;
        let per_byte = 2 * std::mem::size_of::<Vec<i16>>() + 8 * std::mem::size_of::<i16>() + 32;
        encoded
            .checked_mul(per_byte)
            .and_then(|size| size.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "node too large"))
    }

    async fn load(
        _: &std::path::Path,
        file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        tbon::de::read_from((), file)
            .await
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

impl freqfs::FileSave for File {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        use futures::TryStreamExt;
        use tokio::io::AsyncWriteExt;

        let mut stream = tbon::en::encode(self).map_err(std::io::Error::other)?;
        let mut size = 0;

        while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }

        Ok(size)
    }
}
