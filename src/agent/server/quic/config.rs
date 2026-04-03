use crate::util::error::{HarDataError, Result};
use quinn::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;
use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::prelude::FromDer;

pub use crate::agent::server::common::{
    MAX_CONCURRENT_CONNECTIONS, MAX_PAYLOAD_SIZE, REQUEST_TIMEOUT_SECS,
};

pub const MAX_CONCURRENT_STREAMS: usize = 10000;
const TLS_DIR_NAME: &str = "tls";

#[derive(Debug, Serialize, Deserialize)]
struct CertificateHostnamesMetadata {
    hostnames: Vec<String>,
}

fn tls_paths(_data_dir: &Path, primary_hostname: &str) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let tls_dir = PathBuf::from(".hardata").join(TLS_DIR_NAME);
    let safe_hostname = primary_hostname.replace(':', "-");
    let cert_path = tls_dir.join(format!("agent-cert-{}.der", safe_hostname));
    let key_path = tls_dir.join(format!("agent-key-{}.der", safe_hostname));
    let hostnames_path = tls_dir.join(format!("agent-cert-{}-hostnames.json", safe_hostname));
    (tls_dir, cert_path, key_path, hostnames_path)
}

fn normalize_hostname(hostname: &str) -> Option<String> {
    let trimmed = hostname.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(addr) = trimmed.parse::<SocketAddr>() {
        return Some(addr.ip().to_string());
    }

    let normalized = strip_host_port(trimmed).trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn strip_host_port(value: &str) -> &str {
    if let Some(rest) = value.strip_prefix('[') {
        if let Some((host, suffix)) = rest.split_once(']') {
            if suffix.is_empty() || suffix.starts_with(':') {
                return host;
            }
        }
    }

    if let Some((host, port)) = value.rsplit_once(':') {
        if !host.is_empty()
            && !port.is_empty()
            && port.chars().all(|c| c.is_ascii_digit())
            && !host.contains(':')
        {
            return host;
        }
    }

    value
}

fn build_certificate_hostnames(bind_addr: &str, configured_hostnames: &[String]) -> Vec<String> {
    let mut names = BTreeSet::from([
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ]);

    if let Ok(addr) = bind_addr.parse::<SocketAddr>() {
        if !addr.ip().is_unspecified() {
            names.insert(addr.ip().to_string());
        }
    } else {
        let host = bind_addr
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(bind_addr)
            .trim_matches('[')
            .trim_matches(']');

        if !host.is_empty() && host != "0.0.0.0" && host != "::" {
            names.insert(host.to_string());
        }
    }

    for hostname in configured_hostnames {
        if let Some(normalized) = normalize_hostname(hostname) {
            names.insert(normalized);
        }
    }

    names.into_iter().collect()
}

fn load_hostnames_metadata(path: &Path) -> Result<Option<Vec<String>>> {
    if !path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(path).map_err(HarDataError::Io)?;
    let metadata: CertificateHostnamesMetadata = serde_json::from_str(&content).map_err(|e| {
        HarDataError::InvalidConfig(format!("Invalid QUIC certificate metadata: {}", e))
    })?;
    Ok(Some(metadata.hostnames))
}

fn load_hostnames_from_certificate(cert_der: &[u8]) -> Result<Vec<String>> {
    let (_, certificate) =
        x509_parser::certificate::X509Certificate::from_der(cert_der).map_err(|e| {
            HarDataError::InvalidConfig(format!("Failed to parse QUIC certificate DER: {}", e))
        })?;
    let mut hostnames = BTreeSet::new();

    for extension in certificate.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = extension.parsed_extension() {
            for name in &san.general_names {
                match name {
                    GeneralName::DNSName(dns_name) => {
                        if let Some(normalized) = normalize_hostname(dns_name) {
                            hostnames.insert(normalized);
                        }
                    }
                    GeneralName::IPAddress(address) => {
                        let ip = match address.len() {
                            4 => <[u8; 4]>::try_from(*address).ok().map(IpAddr::from),
                            16 => <[u8; 16]>::try_from(*address).ok().map(IpAddr::from),
                            _ => None,
                        };
                        if let Some(ip) = ip {
                            hostnames.insert(ip.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if hostnames.is_empty() {
        return Err(HarDataError::InvalidConfig(
            "Existing QUIC certificate does not contain SAN hostnames".to_string(),
        ));
    }

    Ok(hostnames.into_iter().collect())
}

fn load_existing_cert_material(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(
    Vec<CertificateDer<'static>>,
    PrivateKeyDer<'static>,
    Vec<u8>,
)> {
    let cert_der = fs::read(cert_path).map_err(HarDataError::Io)?;
    let key_der = fs::read(key_path).map_err(HarDataError::Io)?;
    Ok((
        vec![CertificateDer::from(cert_der.clone())],
        PrivateKeyDer::Pkcs8(key_der.into()),
        cert_der,
    ))
}

fn save_hostnames_metadata(path: &Path, hostnames: &[String]) -> Result<()> {
    let metadata = CertificateHostnamesMetadata {
        hostnames: hostnames.to_vec(),
    };
    let content = serde_json::to_vec_pretty(&metadata).map_err(|e| {
        HarDataError::SerializationError(format!(
            "Failed to serialize QUIC certificate metadata: {}",
            e
        ))
    })?;
    fs::write(path, content).map_err(HarDataError::Io)?;
    Ok(())
}

pub fn generate_self_signed_cert_with_hostnames(
    hostnames: Option<Vec<String>>,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let names = hostnames.unwrap_or_else(|| vec!["localhost".to_string()]);
    let cert = rcgen::generate_simple_self_signed(names).map_err(|e| {
        HarDataError::InvalidConfig(format!("Certificate generation failed: {}", e))
    })?;

    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let cert_der = cert.cert.into();

    Ok((vec![cert_der], key))
}

pub fn load_or_generate_self_signed_cert(
    bind_addr: &str,
    data_dir: &Path,
    configured_hostnames: &[String],
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let primary_hostname = configured_hostnames
        .first()
        .map(|s| s.as_str())
        .unwrap_or_else(|| {
            bind_addr
                .split(':')
                .next()
                .unwrap_or("localhost")
        });
    let (tls_dir, cert_path, key_path, hostnames_path) = tls_paths(data_dir, primary_hostname);
    fs::create_dir_all(&tls_dir).map_err(HarDataError::Io)?;
    let subject_alt_names = build_certificate_hostnames(bind_addr, configured_hostnames);
    let has_complete_material = cert_path.exists() && key_path.exists();

    if has_complete_material {
        let metadata_hostnames = load_hostnames_metadata(&hostnames_path)?;
        let (certs, key, cert_der) = load_existing_cert_material(&cert_path, &key_path)?;
        let should_regenerate = match metadata_hostnames.as_ref() {
            Some(existing) => existing != &subject_alt_names,
            None => match load_hostnames_from_certificate(&cert_der) {
                Ok(existing_hostnames) if existing_hostnames == subject_alt_names => {
                    save_hostnames_metadata(&hostnames_path, &existing_hostnames)?;
                    return Ok((certs, key));
                }
                Ok(existing_hostnames) => {
                    warn!(
                            "QUIC certificate metadata missing at {:?}, existing SAN {:?} differs from desired {:?}, regenerating self-signed certificate",
                            tls_dir, existing_hostnames, subject_alt_names
                        );
                    true
                }
                Err(e) => {
                    warn!(
                            "QUIC certificate metadata missing at {:?} and existing SAN could not be parsed ({}), reusing current certificate without rotation",
                            tls_dir, e
                        );
                    return Ok((certs, key));
                }
            },
        };

        if !should_regenerate {
            return Ok((certs, key));
        }

        warn!(
            "QUIC certificate hostnames changed for {:?}, regenerating self-signed certificate",
            tls_dir
        );
    }

    if !has_complete_material && (cert_path.exists() || key_path.exists()) {
        warn!(
            "Incomplete QUIC TLS material found at {:?}, regenerating certificate pair",
            tls_dir
        );
    }

    let (certs, key) = generate_self_signed_cert_with_hostnames(Some(subject_alt_names.clone()))?;
    fs::write(&cert_path, certs[0].as_ref()).map_err(HarDataError::Io)?;
    fs::write(&key_path, key.secret_der()).map_err(HarDataError::Io)?;
    save_hostnames_metadata(&hostnames_path, &subject_alt_names)?;

    Ok((certs, key))
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

#[cfg(test)]
mod tests {
    use super::{build_certificate_hostnames, load_or_generate_self_signed_cert, tls_paths};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-quic-{label}-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn build_certificate_hostnames_merges_explicit_hostnames_for_wildcard_bind() {
        let hostnames = build_certificate_hostnames(
            "0.0.0.0:9443",
            &["files.example.com".to_string(), "10.0.0.8".to_string()],
        );

        assert!(hostnames.contains(&"localhost".to_string()));
        assert!(hostnames.contains(&"127.0.0.1".to_string()));
        assert!(hostnames.contains(&"::1".to_string()));
        assert!(hostnames.contains(&"files.example.com".to_string()));
        assert!(hostnames.contains(&"10.0.0.8".to_string()));
        assert!(!hostnames.contains(&"0.0.0.0".to_string()));
    }

    #[test]
    fn build_certificate_hostnames_strips_ports_from_configured_hostnames() {
        let hostnames = build_certificate_hostnames(
            "0.0.0.0:9443",
            &[
                "files.example.com:9443".to_string(),
                "[2001:db8::1]:9443".to_string(),
                "127.0.0.1:9443".to_string(),
            ],
        );

        assert!(hostnames.contains(&"files.example.com".to_string()));
        assert!(hostnames.contains(&"2001:db8::1".to_string()));
        assert!(hostnames.contains(&"127.0.0.1".to_string()));
        assert!(!hostnames.contains(&"files.example.com:9443".to_string()));
        assert!(!hostnames.contains(&"[2001:db8::1]:9443".to_string()));
    }

    #[test]
    fn load_or_generate_self_signed_cert_regenerates_when_metadata_hostnames_change() {
        let temp_dir = create_temp_dir("quic-cert-regenerate");
        let configured_hostnames = vec!["old.example.com".to_string()];
        load_or_generate_self_signed_cert("0.0.0.0:9443", &temp_dir, &configured_hostnames)
            .unwrap();

        let (_, old_cert_path, _, _) = tls_paths(&temp_dir, "old.example.com");
        let original_cert = fs::read(&old_cert_path).unwrap();
        assert!(!original_cert.is_empty());

        let updated_hostnames = vec!["new.example.com".to_string()];
        load_or_generate_self_signed_cert("0.0.0.0:9443", &temp_dir, &updated_hostnames).unwrap();

        let (_, new_cert_path, _, new_hostnames_path) = tls_paths(&temp_dir, "new.example.com");
        let updated_cert = fs::read(&new_cert_path).unwrap();
        let updated_metadata = fs::read_to_string(&new_hostnames_path).unwrap();
        assert!(!updated_cert.is_empty());
        assert!(updated_metadata.contains("new.example.com"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn load_or_generate_self_signed_cert_backfills_metadata_without_rotating_when_san_matches() {
        let temp_dir = create_temp_dir("quic-cert-missing-metadata");
        load_or_generate_self_signed_cert("127.0.0.1:9443", &temp_dir, &[]).unwrap();

        let (_, cert_path, _, hostnames_path) = tls_paths(&temp_dir, "127.0.0.1");
        let original_cert = fs::read(&cert_path).unwrap();
        fs::remove_file(&hostnames_path).unwrap();

        load_or_generate_self_signed_cert("127.0.0.1:9443", &temp_dir, &[]).unwrap();

        let reused_cert = fs::read(&cert_path).unwrap();
        let regenerated_metadata = fs::read_to_string(&hostnames_path).unwrap();
        assert_eq!(original_cert, reused_cert);
        assert!(regenerated_metadata.contains("127.0.0.1"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn load_or_generate_self_signed_cert_regenerates_when_missing_metadata_and_san_changes() {
        let temp_dir = create_temp_dir("quic-cert-missing-metadata-regenerate");
        load_or_generate_self_signed_cert("127.0.0.1:9443", &temp_dir, &[]).unwrap();

        let (_, old_cert_path, _, old_hostnames_path) = tls_paths(&temp_dir, "127.0.0.1");
        let original_cert = fs::read(&old_cert_path).unwrap();
        assert!(!original_cert.is_empty());

        load_or_generate_self_signed_cert("10.0.0.8:9443", &temp_dir, &[]).unwrap();

        let (_, new_cert_path, _, new_hostnames_path) = tls_paths(&temp_dir, "10.0.0.8");
        let regenerated_cert = fs::read(&new_cert_path).unwrap();
        let regenerated_metadata = fs::read_to_string(&new_hostnames_path).unwrap();
        assert!(!regenerated_cert.is_empty());
        assert!(regenerated_metadata.contains("10.0.0.8"));

        let _ = fs::remove_dir_all(temp_dir);
    }
}
