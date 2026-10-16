# b-tree

A persistent B+ tree using freqfs

Use async `BTreeLock::create` for empty delegated storage, awaiting cache admission
for its root, and `BTreeLock::load` for an existing tree. Loading never creates a
missing root. Synchronize a newly created tree before relying on reopening it
after a restart.

Run `cargo test --all-targets --all-features` to include the strict-load example test.

## Native storage

Mutations update individual blocks in place and remove obsolete nodes. `sync()`
is buffered writeback; `sync_all()` explicitly synchronizes backing storage.
Neither makes multi-block operations atomic across a crash. Durable transaction
history and interrupted-materialization recovery belong to the caller.
`truncate()` removes old nodes before admitting the replacement root; failure or
cancellation may therefore leave incomplete storage which loading rejects.

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

`BTreeWriteGuard::insert_sorted` consumes a fallible ordered key stream beginning
at or beyond the current maximum. It ignores existing equal keys, returns the
number inserted, and retains bounded traversal state. Errors or cancellation may
leave an inserted prefix; callers own unpublished construction and recovery.

Key streams copy the selected rows from one leaf before yielding them, releasing
its payload guard. This conversion is synchronous; node traversal owns read
concurrency. Ordered insertion retains the previous input because collator-equal
but distinct values can change position during a split.

`BTreeWriteGuard::replace` changes one exact row in place, including affected
first-row copies in index nodes. It rejects collator-equivalent ambiguity and
changes that would cross or collide with a neighbor; it never deletes/reinserts.
A replacement may compare differently from its previous value, provided it remains
strictly between its actual predecessor and successor. This public operation is
safe to call directly; callers need no additional ordering guard.
Input checks and affected-node validation complete before synchronous publication,
so rejection or cancellation before publication leaves row contents unchanged.
This is not a durability or cross-index atomicity guarantee.

The private `tree` modules separate reads/traversal, insertion, deletion,
replacement, and stream decoding. The module root owns public handles and shared
node access; operation-specific helpers remain with their operation.
