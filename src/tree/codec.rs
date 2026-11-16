//! Stream decoding delegates native insertion to the tree owner.

use collate::Collate;
use destream::de;
use freqfs::{DirLock, FileLoad};
use futures::TryFutureExt;
use safecast::AsType;

use super::{BTreeLock, Node, Schema};

struct BTreeVisitor<S, C, FE, O> {
    btree: BTreeLock<S, C, FE, O>,
}

impl<S, C, FE, O> de::Visitor for BTreeVisitor<S, C, FE, O>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync,
    O: Collate<Value = S::Value> + Clone + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
    S::Value: de::FromStream<Context = ()>,
{
    type Value = BTreeLock<S, C, FE, O>;

    fn expecting() -> &'static str {
        "a BTree"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut btree = self.btree.write().await;

        while let Some(key) = seq.next_element(()).map_err(de::Error::custom).await? {
            btree.insert(key).map_err(de::Error::custom).await?;
        }

        Ok(self.btree)
    }
}

impl<S, C, FE, O> de::FromStream for BTreeLock<S, C, FE, O>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync,
    O: Collate<Value = S::Value> + Clone + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
    S::Value: de::FromStream<Context = ()>,
{
    type Context = (S, super::IndexOrder<C, O>, DirLock<FE>);

    async fn from_stream<D: de::Decoder>(
        context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        let (schema, ordering, dir) = context;
        let btree = BTreeLock::create(schema, ordering, dir)
            .await
            .map_err(de::Error::custom)?;
        decoder.decode_seq(BTreeVisitor { btree }).await
    }
}
