/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use bytes::{Buf, BufMut};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{
    Status,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
};

pub use protobuf;
use protobuf::Message;

/// A [`Codec`] that implements `application/grpc+proto` via the protobuf
/// library.
#[derive(Debug, Clone)]
pub struct ProtoCodec<T, U> {
    _pd: PhantomData<(T, U)>,
}

impl<T, U> Default for ProtoCodec<T, U> {
    fn default() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T, U> Codec for ProtoCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;

    type Encoder = ProtoEncoder<T>;
    type Decoder = ProtoDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        ProtoEncoder { _pd: PhantomData }
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProtoDecoder { _pd: PhantomData }
    }
}

/// A [`Encoder`] that knows how to encode `T`.
#[derive(Debug, Clone, Default)]
pub struct ProtoEncoder<T> {
    _pd: PhantomData<T>,
}

impl<T> ProtoEncoder<T> {
    /// Get a new encoder with explicit buffer settings
    pub fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T: Message> Encoder for ProtoEncoder<T> {
    type Item = T;
    type Error = Status;

    const ENCODE_READY: bool = true;

    fn encode_ready(
        self: Pin<&mut Self>,
        item: Self::Item,
        mut buf: EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let _ = self;
        // The protobuf library doesn't support serializing into a user-provided
        // buffer. Instead, it allocates its own buffer, resulting in an extra
        // copy and allocation.
        // TODO: #2345 - Find a way to avoid this extra copy.
        item.serialize()
            .map_err(from_decode_error)
            .map(|serialized| buf.put_slice(serialized.as_slice()))
    }
}

/// A [`Decoder`] that knows how to decode `U`.
#[derive(Debug, Clone, Default)]
pub struct ProtoDecoder<U> {
    _pd: PhantomData<U>,
}

impl<U> ProtoDecoder<U> {
    /// Get a new decoder.
    pub fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<U: Message + Default> Decoder for ProtoDecoder<U> {
    type Item = U;
    type Error = Status;

    fn poll_decode(
        &mut self,
        _cx: &mut Context<'_>,
        mut buf: DecodeBuf<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let slice = buf.chunk();
        let item = U::parse(slice).map_err(from_decode_error);
        buf.advance(slice.len());
        Poll::Ready(item.map(Some))
    }
}

fn from_decode_error(error: impl std::error::Error) -> tonic::Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::internal(error.to_string())
}
