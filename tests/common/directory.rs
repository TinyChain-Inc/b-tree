//! Directory ownership for examples and their tests, independent of cache policy.

use std::io;
use std::path::{Path, PathBuf};

use rand::RngExt;

pub(super) struct TestDir(PathBuf);

impl TestDir {
    pub(super) fn new(prefix: &str) -> io::Result<Self> {
        loop {
            let path =
                std::env::temp_dir().join(format!("{prefix}_{}", rand::rng().random::<u64>()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.0
    }

    // Call after releasing storage handles. Drop also cleans up on errors or unwinding.
    pub(super) fn close(self) -> io::Result<()> {
        match std::fs::remove_dir_all(&self.0) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn directory_cleanup_on_completion_and_unwinding() -> io::Result<()> {
    let dir = TestDir::new("directory_close")?;
    let path = dir.path().to_owned();
    dir.close()?;
    assert!(!path.exists());

    let dir = TestDir::new("directory_removed")?;
    std::fs::remove_dir(dir.path())?;
    dir.close()?;

    let dir = TestDir::new("directory_unwind")?;
    let path = dir.path().to_owned();
    assert!(
        std::panic::catch_unwind(move || {
            let _dir = dir;
            panic!("injected fixture failure");
        })
        .is_err()
    );
    assert!(!path.exists());
    Ok(())
}
