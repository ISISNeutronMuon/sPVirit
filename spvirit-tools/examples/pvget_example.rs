use spvirit_tools::{PvGetOptions, RenderOptions, format_output, pvget};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pv_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MY:PV:NAME".into());

    let opts = PvGetOptions::new(pv_name);
    let result = pvget(&opts).await?;

    let render = RenderOptions::default();
    println!("{}", format_output(&result.pv_name, &result.value, &render));
    Ok(())
}
