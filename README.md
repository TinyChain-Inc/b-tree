# b-tree
A persistent B+ tree using freqfs

Use `BTreeLock::create` for empty delegated storage and `BTreeLock::load` for an
existing tree. Loading never creates a missing root. Synchronize a newly created
tree before relying on reopening it after a restart.

Run `cargo test --all-targets --all-features` to include the strict-load example test.

## Filesystem codecs

File entries implement `freqfs::FileLoad` and `FileSave`. Loads reconstruct the
same entry type that saves write; typed access validates the resulting entry via
`AsType`. Adapters must preserve payload identity across persistence rather than
reinterpret bytes as whichever type a reader requests.

The `stream` feature supplies destream implementations without selecting a byte
codec. Applications implement `FileLoad`/`FileSave` for their file entry type
using their chosen codec. The examples choose TBON explicitly.
