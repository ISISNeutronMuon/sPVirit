use std::net::SocketAddr;
use std::time::Duration;

use argparse::{ArgumentParser, Store, StoreTrue};

use super::search::{is_auto_addr_list_enabled, parse_name_servers};
use super::types::PvGetOptions;

/// Common CLI arguments shared across all PVA client tools.
///
/// Usage:
/// ```ignore
/// let mut common = CommonClientArgs::new();
/// // optionally adjust defaults before parsing:
/// // common.timeout_secs = 3;
/// {
///     let mut ap = ArgumentParser::new();
///     common.add_to_parser(&mut ap);
///     // add tool-specific args here
///     ap.parse_args_or_exit();
/// }
/// common.init_tracing();
/// let opts = common.into_pv_get_options("myPV".to_string())?;
/// ```
pub struct CommonClientArgs {
    pub timeout_secs: u64,
    pub server: String,
    pub search_addr: String,
    pub bind_addr: String,
    pub name_server: String,
    pub udp_port: u16,
    pub tcp_port: u16,
    pub debug: bool,
    pub no_broadcast: bool,
    pub authnz_user: String,
    pub authnz_host: String,
}

impl CommonClientArgs {
    pub fn new() -> Self {
        Self {
            timeout_secs: 5,
            server: String::new(),
            search_addr: String::new(),
            bind_addr: String::new(),
            name_server: String::new(),
            udp_port: 5076,
            tcp_port: 5075,
            debug: false,
            no_broadcast: false,
            authnz_user: String::new(),
            authnz_host: String::new(),
        }
    }

    /// Register common flags on the given parser.
    ///
    /// Short flags `-w` and `-d` are included so that tools like `pvlist`
    /// that historically accepted them keep working.
    pub fn add_to_parser<'a, 'b>(&'a mut self, ap: &'b mut ArgumentParser<'a>) {
        ap.refer(&mut self.timeout_secs).add_option(
            &["-w", "--timeout"],
            Store,
            "Timeout in seconds",
        );
        ap.refer(&mut self.server)
            .add_option(&["--server"], Store, "Server address (ip:port)");
        ap.refer(&mut self.search_addr).add_option(
            &["--search-addr"],
            Store,
            "Search target IP (default EPICS_PVA_ADDR_LIST/auto broadcast)",
        );
        ap.refer(&mut self.bind_addr).add_option(
            &["--bind-addr"],
            Store,
            "Local bind IP for UDP search",
        );
        ap.refer(&mut self.name_server).add_option(
            &["--name-server"],
            Store,
            "PVA name server address (host:port, repeatable via EPICS_PVA_NAME_SERVERS)",
        );
        ap.refer(&mut self.udp_port)
            .add_option(&["--udp-port"], Store, "UDP search port");
        ap.refer(&mut self.tcp_port)
            .add_option(&["--tcp-port"], Store, "TCP server default port");
        ap.refer(&mut self.debug)
            .add_option(&["-d", "--debug"], StoreTrue, "Enable debug logging");
        ap.refer(&mut self.no_broadcast).add_option(
            &["--no-broadcast"],
            StoreTrue,
            "Disable UDP broadcast/multicast search (also set via EPICS_PVA_AUTO_ADDR_LIST=NO)",
        );
        ap.refer(&mut self.authnz_user).add_option(
            &["--authnz-user"],
            Store,
            "AuthNZ user override (takes precedence over env)",
        );
        ap.refer(&mut self.authnz_host).add_option(
            &["--authnz-host"],
            Store,
            "AuthNZ host override (takes precedence over env)",
        );
    }

    /// Initialise the `tracing_subscriber` based on `--debug`.
    pub fn init_tracing(&self) {
        let max_level = if self.debug {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        };
        tracing_subscriber::fmt().with_max_level(max_level).init();
    }

    /// Convert the parsed CLI strings into a ready-to-use `PvGetOptions`.
    pub fn into_pv_get_options(
        self,
        pv_name: String,
    ) -> Result<PvGetOptions, Box<dyn std::error::Error>> {
        let mut opts = PvGetOptions::new(pv_name);
        opts.timeout = Duration::from_secs(self.timeout_secs);
        opts.udp_port = self.udp_port;
        opts.tcp_port = self.tcp_port;
        opts.debug = self.debug;
        opts.no_broadcast = self.no_broadcast || !is_auto_addr_list_enabled();

        if !self.server.is_empty() {
            let addr: SocketAddr = self.server.parse()?;
            opts.server_addr = Some(addr);
        }
        if !self.search_addr.is_empty() {
            opts.search_addr = Some(self.search_addr.parse()?);
        }
        if !self.bind_addr.is_empty() {
            opts.bind_addr = Some(self.bind_addr.parse()?);
        }

        let mut ns = parse_name_servers(&self.name_server);
        if let Ok(env) = std::env::var("EPICS_PVA_NAME_SERVERS") {
            ns.extend(parse_name_servers(&env));
        }
        opts.name_servers = ns;

        if !self.authnz_user.is_empty() {
            opts.authnz_user = Some(self.authnz_user);
        }
        if !self.authnz_host.is_empty() {
            opts.authnz_host = Some(self.authnz_host);
        }

        Ok(opts)
    }
}
