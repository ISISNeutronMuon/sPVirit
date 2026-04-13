use argparse::{ArgumentParser, Store, StoreTrue};
use tokio::runtime::Runtime;

use spvirit_tools::spvirit_client::cli::CommonClientArgs;
use spvirit_tools::spvirit_client::client::pvget;
use spvirit_tools::spvirit_client::format::{OutputFormat, RenderOptions, format_output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pv_name = String::new();
    let mut raw = false;
    let mut json = false;
    let mut common = CommonClientArgs::new();

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Minimal PVA pvget client");
        ap.refer(&mut pv_name)
            .add_argument("pv", Store, "PV name to fetch");
        common.add_to_parser(&mut ap);
        ap.refer(&mut raw)
            .add_option(&["--raw"], StoreTrue, "Print raw hex payload");
        ap.refer(&mut json)
            .add_option(&["--json"], StoreTrue, "Print JSON output");
        ap.parse_args_or_exit();
    }

    common.init_tracing();
    let opts = common.into_pv_get_options(pv_name.clone())?;

    let rt = Runtime::new()?;
    let result = rt.block_on(pvget(&opts))?;

    let mut render_opts = RenderOptions::default();
    if json {
        render_opts.format = OutputFormat::Json;
    }
    println!(
        "{}",
        format_output(&result.pv_name, &result.value, &render_opts)
    );

    if raw {
        println!("raw_pva: {}", hex::encode(result.raw_pva));
        println!("raw_pvd: {}", hex::encode(result.raw_pvd));
    }

    Ok(())
}
