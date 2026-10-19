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

`validate()` owns whole-tree structural, ordering, and identity checks. `copy_into()`
copies reachable nodes into empty delegated storage; callers coordinate source
access. Neither introduces immutable root generations or version reclamation.

Leaves own complete values. Each internal node owns one routing separator between
neighboring children, so it has one more child than separator. Separators retain
the shortest right-row prefix which distinguishes the adjacent rows; fully
collation-equivalent rows require a full-width separator. An omitted suffix means
negative infinity only for routing, not for public prefix queries.

Schemas validate supplied separator columns through `validate_prefix`; adapters
own their serialization. The former internal-node structure with one copied
minimum per child is incompatible and is rejected by structural validation.
Recreate storage containing those internal nodes; loading never migrates it.
Leaf encoding, logical row streams, and payload tags are unchanged.

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
concurrency. Ordered insertion checks duplicates across the equivalent group
when the next input ties the previous collation.

[`BTreeWriteGuard::replace`](https://docs.rs/b-tree/latest/b_tree/struct.BTree.html#method.replace)
changes one row in place while preserving its position and the tree topology.
Its API documentation defines collision checks and the publication boundary;
callers remain responsible for durability and cross-index recovery.

The private `tree` modules separate reads/traversal, insertion, deletion,
replacement, and stream decoding. The module root owns public handles and shared
node access; operation-specific helpers remain with their operation.

`get(prefix)` resolves one semantic prefix and rejects multiple matching rows.
`contains`, insertion, deletion, and replacement search equivalent groups without
collecting them; traversal retains a path proportional to tree height. Ranges and
stream grouping retain their collation-based meaning. Collators must order equal
values equally, but may group unequal values.

Equivalent-group navigation reuses acquired leaf guards and retains only a
root-to-leaf path. Lookup is linear in the size of a collation-equivalent group;
strict validation's exact duplicate checks can require quadratic equality work
within that group. Replacement combines identity and collision checks when both
values belong to the same group, and releases read guards before publication.
