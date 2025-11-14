use crate::util::error::{HarDataError, Result};
use quinn::ServerConfig;
use std::sync::Arc;

pub use crate::agent::server::common::{
    MAX_CONCURRENT_CONNECTIONS, MAX_PAYLOAD_SIZE, REQUEST_TIMEOUT_SECS,
};

pub const MAX_CONCURRENT_STREAMS: usize = 10000;

pub fn generate_self_signed_cert_with_hostnames(
    hostnames: Option<Vec<String>>,
) -> Result<(
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let names = hostnames.unwrap_or_else(|| vec!["localhost".to_string()]);
    let cert = rcgen::generate_simple_self_signed(names).map_err(|e| {
        HarDataError::InvalidConfig(format!("Certificate generation failed: {}", e))
    })?;

    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let cert_der = cert.cert.into();

    Ok((vec![cert_der], key))
}

pub fn generate_self_signed_cert() -> Result<(
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    generate_self_signed_cert_with_hostnames(None)
}

pub fn configure_server(
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<ServerConfig> {
    let mut server_config = ServerConfig::with_single_cert(certs, key)
        .map_err(|e| HarDataError::InvalidConfig(format!("Server config error: {}", e)))?;

    let transport_config = Arc::get_mut(&mut server_config.transport).ok_or_else(|| {
        HarDataError::InvalidConfig("Failed to get mutable transport config".to_string())
    })?;

    if let Ok(timeout) = std::time::Duration::from_secs(600).try_into() {
        transport_config.max_idle_timeout(Some(timeout));
    }

    transport_config.max_concurrent_bidi_streams(1000u32.into());
    transport_config.max_concurrent_uni_streams(1000u32.into());

    transport_config.stream_receive_window(quinn::VarInt::from_u32(50 * 1024 * 1024));
    if let Ok(window) = quinn::VarInt::from_u64(200 * 1024 * 1024) {
        transport_config.receive_window(window);
    }

    if let Ok(window) = quinn::VarInt::from_u64(200 * 1024 * 1024) {
        transport_config.send_window(window.into_inner());
    }

    Ok(server_config)
}
