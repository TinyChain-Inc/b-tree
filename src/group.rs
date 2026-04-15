use collate::Collate;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::stream::{Fuse, Stream, StreamExt};
use pin_project::pin_project;

use crate::tree::Leaf;
use crate::{Collator, Key, NODE_STACK_SIZE};

#[pin_project]
pub struct GroupBy<C, S, V> {
    #[pin]
    source: Fuse<S>,
    collator: Collator<C>,
    len: usize,
    reverse: bool,
    pending: VecDeque<Key<V>>,
}

impl<C, S, FE, V> GroupBy<C, S, V>
where
    S: Stream<Item = Result<Leaf<FE, V>, io::Error>>,
{
    pub fn new(collator: Collator<C>, source: S, n: usize, reverse: bool) -> Self {
        Self {
            source: source.fuse(),
            collator,
            len: n,
            reverse,
            pending: VecDeque::with_capacity(NODE_STACK_SIZE),
        }
    }
}

impl<C, S, V> GroupBy<C, S, V>
where
    C: Collate<Value = V>,
    V: Clone,
{
    fn append_leaf<'a, I>(pending: &mut VecDeque<Key<V>>, collator: &Collator<C>, n: usize, leaf: I)
    where
        I: IntoIterator<Item = &'a Vec<V>>,
        V: 'a,
    {
        for key in leaf {
            let key = &key[..n];

            let key: Option<Key<V>> = if let Some(back) = pending.back() {
                if collator.cmp_slices(back, key) == Ordering::Equal {
                    None
                } else {
                    Some(key.iter().cloned().collect())
                }
            } else {
                Some(key.iter().cloned().collect())
            };

            if let Some(key) = key {
                pending.push_back(key.iter().cloned().collect());
            }
        }
    }
}

impl<C, S, FE, V> Stream for GroupBy<C, S, V>
where
    C: Collate<Value = V>,
    S: Stream<Item = Result<Leaf<FE, V>, io::Error>> + Unpin,
    V: Clone,
{
    type Item = Result<Key<V>, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cxt: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        Poll::Ready({
            loop {
                if let Some(key) = this.pending.pop_front() {
                    break Some(Ok(key));
                }

                match ready!(this.source.as_mut().poll_next(cxt)) {
                    Some(Ok(leaf)) => {
                        if *this.reverse {
                            Self::append_leaf(
                                this.pending,
                                this.collator,
                                *this.len,
                                leaf.as_ref().iter().rev(),
                            )
                        } else {
                            Self::append_leaf(this.pending, this.collator, *this.len, leaf.as_ref())
                        }
                    }
                    Some(Err(cause)) => break Some(Err(cause)),
                    None => break None,
                }
            }
        })
    }
}
