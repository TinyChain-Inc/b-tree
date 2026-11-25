//! Stream decoding delegates native insertion to the tree owner.

use collate::Collate;
use destream::de;
use freqfs::{DirLock, FileLoad};
use futures::TryFutureExt;
use safecast::AsType;

use super::{BTreeLock, Node, Schema};

struct BTreeVisitor<S, C, FE> {
    btree: BTreeLock<S, C, FE>,
}

impl<S, C, FE> de::Visitor for BTreeVisitor<S, C, FE>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
    S::Value: de::FromStream<Context = ()>,
{
    type Value = BTreeLock<S, C, FE>;

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

impl<S, C, FE> de::FromStream for BTreeLock<S, C, FE>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
    S::Value: de::FromStream<Context = ()>,
{
    type Context = (S, C, DirLock<FE>);

    async fn from_stream<D: de::Decoder>(
        context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        let (schema, collator, dir) = context;
        let btree = BTreeLock::create(schema, collator, dir)
            .await
            .map_err(de::Error::custom)?;
        decoder.decode_seq(BTreeVisitor { btree }).await
    }
}
