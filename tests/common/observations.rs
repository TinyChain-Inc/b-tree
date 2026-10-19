// Test-only observation counters, excluded from non-test builds. Each test
// scopes its own task-local values so concurrent tests do not share state.
tokio::task_local! {
    pub(crate) static LEAF_READS: std::cell::Cell<usize>;
    pub(crate) static COLLATIONS: std::cell::Cell<usize>;
    pub(crate) static INDEX_WRITES: std::cell::Cell<usize>;
}
