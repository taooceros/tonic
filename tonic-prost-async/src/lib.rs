#![doc(html_root_url = "https://docs.rs/tonic-prost-async/0.14.6")]

//! Async prost codec implementation for tonic.

mod codec;

pub use codec::{ProstCodec, ProstDecoder, ProstEncode, ProstEncoder};
