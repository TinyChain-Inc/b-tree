use std::marker::PhantomData;
use std::{fmt, io};

use destream::{de, en};
use futures::TryFutureExt;
use safecast::AsType;

use b_tree::{Node, Schema};

#[cfg(test)]
use crate::observations::{INDEX_WRITES, LEAF_READS};

pub(super) const BLOCK_SIZE: usize = 4_096;

#[derive(Clone)]
pub(super) enum File {
    Node(Node<Vec<Vec<i16>>>),
}

impl de::FromStream for File {
    type Context = ();

    async fn from_stream<D: de::Decoder>(cxt: (), decoder: &mut D) -> Result<Self, D::Error> {
        Node::from_stream(cxt, decoder).map_ok(Self::Node).await
    }
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Node(node) => node.to_stream(encoder),
        }
    }
}

impl From<Node<Vec<Vec<i16>>>> for File {
    fn from(node: Node<Vec<Vec<i16>>>) -> Self {
        Self::Node(node)
    }
}

impl AsType<Node<Vec<Vec<i16>>>> for File {
    fn as_type(&self) -> Option<&Node<Vec<Vec<i16>>>> {
        let Self::Node(node) = self;

        #[cfg(test)]
        if matches!(node, Node::Leaf(_)) {
            let _ = LEAF_READS.try_with(|reads| reads.set(reads.get() + 1));
        }

        Some(node)
    }

    fn as_type_mut(&mut self) -> Option<&mut Node<Vec<Vec<i16>>>> {
        let Self::Node(node) = self;
        #[cfg(test)]
        if matches!(node, Node::Index(..)) {
            let _ = INDEX_WRITES.try_with(|writes| writes.set(writes.get() + 1));
        }
        Some(node)
    }

    fn into_type(self) -> Option<Node<Vec<Vec<i16>>>> {
        let Self::Node(node) = self;
        Some(node)
    }
}

#[derive(Debug)]
pub(super) struct ExampleSchema<T> {
    pub(super) size: usize,
    pub(super) order: usize,
    pub(super) value: PhantomData<T>,
}

impl<T> ExampleSchema<T> {
    pub(super) fn new(size: usize) -> Self {
        Self {
            size,
            order: 5,
            value: PhantomData,
        }
    }
}

impl<T> PartialEq for ExampleSchema<T> {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.order == other.order
    }
}

impl<T> Eq for ExampleSchema<T> {}

impl<T: fmt::Debug> Schema for ExampleSchema<T> {
    type Error = io::Error;
    type Value = i16;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn len(&self) -> usize {
        self.size
    }

    fn order(&self) -> usize {
        self.order
    }

    fn validate_prefix(&self, prefix: &[i16]) -> Result<(), io::Error> {
        if !prefix.is_empty() && prefix.len() <= self.size {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid separator width",
            ))
        }
    }

    fn validate_key(&self, key: Vec<i16>) -> Result<Vec<i16>, io::Error> {
        if key.len() == self.size {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("key length should be {}", self.size),
            ))
        }
    }
}

impl get_size::GetSize for File {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Node(node) => node.get_heap_size(),
        }
    }
}

impl freqfs::FileLoad for File {
    async fn load_size(
        _: &std::path::Path,
        _: &mut tokio::fs::File,
        metadata: &std::fs::Metadata,
    ) -> io::Result<usize> {
        // TBON sequences have no allocation-sized length header. Every retained
        // row/cell needs encoded input; cover geometric Vec growth and scalar
        // payload bytes before decoding without retaining any parsed payload.
        let encoded = usize::try_from(metadata.len()).map_err(io::Error::other)?;
        let per_byte = 2 * std::mem::size_of::<Vec<i16>>() + 8 * std::mem::size_of::<i16>() + 32;
        encoded
            .checked_mul(per_byte)
            .and_then(|size| size.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "node too large"))
    }

    async fn load(
        _: &std::path::Path,
        file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        tbon::de::read_from((), file)
            .await
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

impl freqfs::FileSave for File {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        use futures::TryStreamExt;
        use tokio::io::AsyncWriteExt;

        let mut stream = tbon::en::encode(self).map_err(std::io::Error::other)?;
        let mut size = 0;

        while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }

        Ok(size)
    }
}
