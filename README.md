# b-tree

A persistent B+ tree using freqfs

First, last, and emptiness share the same validated edge traversal. First and
last return owned rows; emptiness consumes only presence and does not copy the
matching row. Storage guards remain scoped to traversal.

Use async `BTreeLock::create` for empty delegated storage, awaiting cache admission
for its root, and `BTreeLock::load` for an existing tree. Loading never creates a
missing root. Synchronize a newly created tree before relying on reopening it
after a restart.

Run `cargo test --all-targets --all-features` to include the native integration tests.

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

Ordinary readers validate the width of each visited leaf before selection,
counting, or borrowed callbacks. Malformed stored rows return typed integrity
errors; native callers own recovery. Full structural validation remains separate.

Leaves own complete values. Each internal node owns one routing separator between
neighboring children, so it has one more child than separator. Separators retain
the shortest right-row prefix which distinguishes the adjacent rows; fully
collation-equivalent rows require a full-width separator. An omitted suffix means
negative infinity only for routing, not for public prefix queries: `[7]` matches
the prefix of `[7, 42]`, but sorts before it during routing.

Callers supply value comparison; `Collator` supplies key-prefix comparison,
separator construction, and routing. Collation equality can group distinct
values, so exact-row matching checks semantic identity separately. `Range` owns
inclusive/exclusive endpoint comparison.

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
its payload guard. `select` uses the same traversal and copies only requested
columns, preserving row multiplicity and range filtering on complete keys.
This conversion is synchronous; node traversal owns read concurrency.
`visit` uses that traversal to lend complete rows to a synchronous callback,
including callbacks and range values borrowed from the caller. The callback
cannot retain row references or reacquire storage protected by the tree owner.
It may stop with `ControlFlow::Break`; all scan-owned guards are released on
completion, error, cancellation, or early stop. Owned streams remain independent
of callback lifetimes and continue releasing leaf guards before yielding values.

Ordered insertion checks duplicates across the equivalent group when the next
input ties the previous collation.

[`BTreeWriteGuard::replace`](https://docs.rs/b-tree/latest/b_tree/struct.BTree.html#method.replace)
changes one row in place while preserving its position and the tree topology.
Its API documentation defines collision checks and the publication boundary;
callers remain responsible for durability and cross-index recovery.

The private `tree` modules separate reads/traversal, insertion, deletion,
replacement, and stream decoding. The module root owns public handles and shared
node access; operation-specific helpers remain with their operation.

`get(prefix)` resolves one semantic prefix and rejects multiple matching rows.
`get_with(prefix, callback)` lends the unique row after completing the same check,
so missing or ambiguous rows never invoke the callback. It releases lookup guards
before reacquiring the selected leaf, trading one additional read for avoiding
value clones. The owned `get` retains its existing acquisition count.
`contains`, insertion, deletion, and replacement search equivalent groups without
collecting them; traversal retains a path proportional to tree height. Ranges and
stream grouping retain their collation-based meaning. Collators must order equal
values equally, but may group unequal values.

Range containment includes descendant prefixes only when the ancestor's bounds
include their fixed column value. Included and excluded endpoints use the same
comparison for keys and descendant ranges; equal-length prefixes compare bounds.

Equivalent-group navigation reuses acquired leaf guards and retains only a
root-to-leaf path. Lookup is linear in the size of a collation-equivalent group;
strict validation's exact duplicate checks can require quadratic equality work
within that group. Replacement combines identity and collision checks when both
values belong to the same group, and releases read guards before publication.

The executable example shares its schema and file adapters from `examples/support/`.
Regression tests live in modules under `tests/native/`, compiled as one `native`
integration target; test-only observations and directory fixtures live in `tests/common/`.

Native range streams can borrow storage within a scope; the owned `Keys` alias
continues to retain static ownership. `BTree::as_ref` borrows the storage owner
instead of cloning its lock. Scoped `visit` traversal also borrows that owner,
including across buffered branches; retained key streams keep their own handles.
Ordinary traversal detects ancestor cycles before descending. Count retains bounded
child IDs and releases parent access before reading children, including with one
available I/O slot.

Visited index nodes validate separator/child cardinality, schema-valid prefixes,
and nondecreasing separator order before routing or mutation. These local checks
use no child reads; strict validation additionally checks occupancy, ancestry,
and subtree boundaries. Malformed stored nodes return marked integrity errors;
native libraries do not select host termination policy.

Lazy scan, projection, and borrowed-visit methods now require schemas
that are `Send + Sync`. Retained streams keep their schema: owned streams share
the existing schema owner, while scoped visits borrow it. Scoped streams require
only that their schema outlive the result; they do not impose `'static` on all
schemas. Point reads retain their existing schema bounds. This is a public Rust
bound change, without a storage-format change or alternate traversal path.

Deletion requires a complete schema-width key; other widths return `InvalidInput`
before lookup or mutation.

Native mutations use insertion, replacement, deletion, and truncation. Cross-tree
`merge` and `delete_all`, and unique-prefix `groups` streams, are no longer public
capabilities. Ordinary key scans, column projection, borrowed consumption,
ordered construction, and independent storage copying remain available.

Each visited index node rejects repeated child IDs through the shared local
validator; collation-equivalent separator keys remain supported. Ordered insertion
uses the same leaf-width validation as ordinary reads, including after input polling
before append, and checks ancestry during its rightmost descent. Malformed acquired
nodes return marked integrity errors before their contents are used. Index uniqueness
checks use a transient node-sized set of borrowed IDs; rightmost descent retains
only its ancestry IDs, without payload copies or a persistent node registry.

Fresh mutation-node acquisition shares the leaf-width and local index validators
used by reads, including siblings acquired only for deletion rebalancing. Recursive
mutations rely on that boundary rather than repeating the same checks. Nodes
already held by deletion may be transiently underfull; acquisition adds no occupancy
or whole-tree validation. A late sibling error rejects consumption of that sibling,
but earlier local changes may already have occurred. Native callers remain
responsible for recovery after interrupted or failed mutation.
