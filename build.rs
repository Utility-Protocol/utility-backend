fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure()
        .compile(
            &[
                "src/rpc/proto/telemetry.proto",
                "src/rpc/proto/clock_sync.proto",
                "src/rpc/proto/watermark.proto",
                "src/rpc/proto/gossip.proto",
            ],
            &["src/rpc/proto"],
        )?;
    Ok(())
}
