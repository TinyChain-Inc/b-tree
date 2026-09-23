# b-tree
A persistent B+ tree using freqfs

Use `BTreeLock::create` for empty delegated storage and `BTreeLock::load` for an
existing tree. Loading never creates a missing root. Synchronize a newly created
tree before relying on reopening it after a restart.

Run `cargo test --all-targets --all-features` to include the strict-load example test.

## Native storage

Mutations update individual blocks in place and remove obsolete nodes. `sync()`
is buffered writeback; `sync_all()` explicitly synchronizes backing storage.
Neither makes multi-block operations atomic across a crash. Durable transaction
history and interrupted-materialization recovery belong to the caller.

`validate()` checks reachable structure, ordering and node identities. `copy_into()`
copies reachable nodes into empty delegated storage; callers coordinate source
access. Neither introduces immutable root generations or version reclamation.

## Filesystem codecs

File entries implement `freqfs::FileLoad` and `FileSave`. Loads reconstruct the
same entry type that saves write; typed access validates the resulting entry via
`AsType`. Adapters must preserve payload identity across persistence rather than
reinterpret bytes as whichever type a reader requests.

The `stream` feature supplies destream implementations without selecting a byte
codec. Applications implement `FileLoad`/`FileSave` for their file entry type
using their chosen codec. The examples choose TBON explicitly.
