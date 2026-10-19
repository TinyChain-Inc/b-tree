use std::io;

use collate::Collator;
use freqfs::Cache;
use futures::TryStreamExt;
#[cfg(test)]
use get_size::GetSize;
use smallvec::smallvec;

use self::adapters::{BLOCK_SIZE, ExampleSchema, File};
use self::directory::TestDir;
use self::observations::{COLLATIONS, INDEX_WRITES, LEAF_READS};
use b_tree::{BTreeLock, Node, Range, Schema};

#[path = "../examples/support/adapters.rs"]
mod adapters;
#[path = "common/directory.rs"]
mod directory;
#[path = "common/observations.rs"]
mod observations;

#[path = "native/construction.rs"]
mod construction;
#[path = "native/lifecycle.rs"]
mod lifecycle;
#[path = "native/replacement.rs"]
mod replacement;
#[path = "native/separators.rs"]
mod separators;
