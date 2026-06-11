fn main() {
    prost_build::compile_protos(&["proto/async_payload_encode.proto"], &["proto"])
        .expect("compile async payload encode benchmark protobuf");
}
