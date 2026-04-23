use spvirit_client::PvaClient;
use std::ops::ControlFlow;

/// Minimal `pvmonitor` example.
///
/// Usage:
///
/// ```text
/// cargo run --example pvmonitor -- MY:PV
/// cargo run --example pvmonitor -- MY:PV --fields value,alarm.severity
/// ```
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut pv: Option<String> = None;
    let mut fields: Vec<String> = Vec::new();

    while let Some(a) = args.next() {
        match a.as_str() {
            "-F" | "--fields" => {
                let raw = args.next().ok_or("--fields requires a value")?;
                fields.extend(
                    raw.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty()),
                );
            }
            _ if pv.is_none() => pv = Some(a),
            other => return Err(format!("unexpected argument: {other}").into()),
        }
    }

    let pv = pv.unwrap_or_else(|| "MY:PV:NAME".into());
    let client = PvaClient::builder().build();
    let cb = |value: &spvirit_codec::spvd_decode::DecodedValue| {
        println!("{value}");
        ControlFlow::Continue(())
    };
    if fields.is_empty() {
        client.pvmonitor(&pv, cb).await?;
    } else {
        let refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        client.pvmonitor_fields(&pv, &refs, cb).await?;
    }
    Ok(())
}
