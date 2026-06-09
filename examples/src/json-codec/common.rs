//! This module defines common request/response types as well as the JsonCodec that is used by the
//! json.helloworld.Greeter service which is defined manually (instead of via proto files) by the
//! `build_json_codec_service` function in the `examples/build.rs` file.

use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use std::{
    future::{Ready, ready},
    marker::PhantomData,
    pin::Pin,
};
use tonic::{
    Status,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuffer, Encoder, ReadyEncode},
};

#[derive(Debug, Deserialize, Serialize)]
pub struct HelloRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HelloResponse {
    pub message: String,
}

#[derive(Debug)]
pub struct JsonEncoder<T>(PhantomData<T>);

impl<T: serde::Serialize> Encoder for JsonEncoder<T> {
    type Item = T;
    type Error = Status;
    type Encode = ReadyEncode<Status>;

    fn encode(
        self: Pin<&mut Self>,
        item: Self::Item,
        mut buf: EncodeBuffer,
    ) -> Result<Self::Encode, Self::Error> {
        let _ = self;
        serde_json::to_writer(buf.as_encode_buf().writer(), &item)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(ReadyEncode::new(buf))
    }
}

#[derive(Debug)]
pub struct JsonDecoder<U>(PhantomData<U>);

impl<U: serde::de::DeserializeOwned + Send + 'static> Decoder for JsonDecoder<U> {
    type Item = U;
    type Error = Status;
    type Decode = Ready<Result<Option<U>, Status>>;

    fn decode(self: Pin<&mut Self>, buf: DecodeBuf<'_>) -> Result<Self::Decode, Self::Error> {
        let _ = self;
        if !buf.has_remaining() {
            return Ok(ready(Ok(None)));
        }

        let item =
            serde_json::from_reader(buf.reader()).map_err(|e| Status::internal(e.to_string()))?;
        Ok(ready(Ok(Some(item))))
    }
}

/// A [`Codec`] that implements `application/grpc+json` via the serde library.
#[derive(Debug, Clone)]
pub struct JsonCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Default for JsonCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for JsonCodec<T, U>
where
    T: serde::Serialize + Send + 'static,
    U: serde::de::DeserializeOwned + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = JsonEncoder<T>;
    type Decoder = JsonDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        JsonEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        JsonDecoder(PhantomData)
    }
}
