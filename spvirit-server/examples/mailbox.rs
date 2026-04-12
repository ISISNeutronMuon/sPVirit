use spvirit_server::PvaServer;

/// p4p SharedPV mailbox equivalent in spvirit-server.
///
/// p4p pattern:
/// - SharedPV(nt=NTScalar('d'), initial=0.0)
/// - put handler does pv.post(op.value()); op.done()
///
/// spvirit pattern:
/// - declare writable scalar PV with .ao(...)
/// - protocol PUT already updates stored value and notifies monitors
/// - optional .on_put(...) callback for side effects/logging
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = PvaServer::builder()
        .ao("demo:pv:name", 0.0)
        .on_put("demo:pv:name", |pv, val| {
            println!("PUT {pv} <- {val:?}");
        })
        .build();

    println!("Serving mailbox PV: demo:pv:name");
    println!("Try: cargo run -p spvirit-client --example pvget -- demo:pv:name");
    println!("Try: cargo run -p spvirit-client --example pvput -- demo:pv:name 3.14");
    println!("Try: cargo run -p spvirit-client --example pvmonitor -- demo:pv:name");

    server.run().await
}
