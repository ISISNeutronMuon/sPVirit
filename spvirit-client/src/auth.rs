use crate::types::PvGetOptions;

pub fn resolved_authnz_user(opts: &PvGetOptions) -> String {
    opts.authnz_user.clone().unwrap_or_else(default_authnz_user)
}

pub fn resolved_authnz_host(opts: &PvGetOptions) -> String {
    opts.authnz_host.clone().unwrap_or_else(default_authnz_host)
}

pub fn default_authnz_user() -> String {
    std::env::var("PVA_AUTHNZ_USER")
        .or_else(|_| std::env::var("USER"))
        .or_else(|_| std::env::var("LOGNAME"))
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

pub fn default_authnz_host() -> String {
    std::env::var("PVA_AUTHNZ_HOST")
        .or_else(|_| std::env::var("HOSTNAME"))
        .or_else(|_| std::env::var("HOST"))
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}
