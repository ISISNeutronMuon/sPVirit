use std::net::{IpAddr, SocketAddr};

use argparse::{ArgumentParser, Store};
use tokio::runtime::Runtime;

use spvirit_client::pvlist::pvlist_with_fallback;
use spvirit_tools::spvirit_client::cli::CommonClientArgs;
use spvirit_tools::spvirit_client::search::{
    DiscoveredServer, build_search_targets, discover_servers,
};

fn format_guid(guid: [u8; 12]) -> String {
    let mut text = String::from("0x");
    for byte in guid {
        text.push_str(&format!("{:02X}", byte));
    }
    text
}

fn parse_server_target(raw: &str, default_port: u16) -> Result<SocketAddr, String> {
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }
    if let Ok(ip) = raw.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, default_port));
    }
    Err(format!(
        "invalid server target '{}'; expected ip:port, ip, or GUID (0x...)",
        raw
    ))
}

fn resolve_server_by_guid(raw: &str, servers: &[DiscoveredServer]) -> Option<SocketAddr> {
    let trimmed = raw.trim();
    if !trimmed.starts_with("0x") {
        return None;
    }
    let hex = &trimmed[2..];
    if hex.len() != 24 {
        return None;
    }

    let mut guid = [0u8; 12];
    for idx in 0..12 {
        let start = idx * 2;
        let part = &hex[start..start + 2];
        let byte = u8::from_str_radix(part, 16).ok()?;
        guid[idx] = byte;
    }

    servers
        .iter()
        .find(|entry| entry.guid == guid)
        .map(|entry| entry.tcp_addr)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut target = String::new();
    let mut common = CommonClientArgs::new();
    common.timeout_secs = 3;

    {
        let mut ap = ArgumentParser::new();
        ap.set_description(
            "PVA pvlist client. Without args prints discovered servers; with target prints PV names from that server.",
        );
        ap.refer(&mut target).add_argument(
            "target",
            Store,
            "Optional server target (ip:port, ip, or GUID starting with 0x)",
        );
        common.add_to_parser(&mut ap);
        ap.parse_args_or_exit();
    }

    common.init_tracing();
    let opts = common.into_pv_get_options("__pvlist".to_string())?;

    let rt = Runtime::new()?;
    let discovered = {
        let targets = build_search_targets(opts.search_addr, opts.bind_addr);
        rt.block_on(discover_servers(
            opts.udp_port,
            opts.timeout,
            &targets,
            opts.debug,
        ))
        .unwrap_or_default()
    };

    if target.trim().is_empty() {
        for server in &discovered {
            println!(
                "GUID {} version 2: tcp@[ {} ]",
                format_guid(server.guid),
                server.tcp_addr
            );
        }
        return Ok(());
    }

    let server_addr = resolve_server_by_guid(target.trim(), &discovered)
        .map(Ok)
        .unwrap_or_else(|| parse_server_target(target.trim(), opts.tcp_port))?;

    let (pv_names, _source) = rt.block_on(pvlist_with_fallback(&opts, server_addr))?;
    for name in pv_names {
        println!("{}", name);
    }

    Ok(())
}
