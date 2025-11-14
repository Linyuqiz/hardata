use crate::core::constants::QUIC_MAX_CONCURRENT_STREAMS;
use crate::sync::net::bandwidth::NetworkQuality;
use crate::util::error::Result;
use quinn::ClientConfig;
use std::sync::Arc;
use std::time::Duration;

pub struct DynamicTimeout {
    pub base_connect_timeout: Duration,
    pub base_read_timeout: Duration,
    pub base_write_timeout: Duration,
    pub rtt_multiplier: f64,
}

impl DynamicTimeout {
    pub fn for_quality(quality: NetworkQuality) -> Self {
        match quality {
            NetworkQuality::Excellent => Self {
                base_connect_timeout: Duration::from_secs(10),
                base_read_timeout: Duration::from_secs(30),
                base_write_timeout: Duration::from_secs(30),
                rtt_multiplier: 1.0,
            },
            NetworkQuality::Good => Self {
                base_connect_timeout: Duration::from_secs(15),
                base_read_timeout: Duration::from_secs(45),
                base_write_timeout: Duration::from_secs(45),
                rtt_multiplier: 1.5,
            },
            NetworkQuality::Fair => Self {
                base_connect_timeout: Duration::from_secs(30),
                base_read_timeout: Duration::from_secs(60),
                base_write_timeout: Duration::from_secs(60),
                rtt_multiplier: 2.0,
            },
            NetworkQuality::Poor | NetworkQuality::Unknown => Self {
                base_connect_timeout: Duration::from_secs(60),
                base_read_timeout: Duration::from_secs(120),
                base_write_timeout: Duration::from_secs(120),
                rtt_multiplier: 4.0,
            },
        }
    }

    pub fn calculate_timeout(&self, operation: OperationType, rtt: Duration) -> Duration {
        let base = match operation {
            OperationType::Connect => self.base_connect_timeout,
            OperationType::Read => self.base_read_timeout,
            OperationType::Write => self.base_write_timeout,
        };

        let rtt_factor = (rtt.as_millis() as f64 / 50.0).max(1.0);
        let adjusted = base.as_millis() as f64 * rtt_factor * self.rtt_multiplier;

        Duration::from_millis(adjusted as u64)
            .max(Duration::from_secs(5))
            .min(Duration::from_secs(300))
    }
}

impl Default for DynamicTimeout {
    fn default() -> Self {
        Self::for_quality(NetworkQuality::Good)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Connect,
    Read,
    Write,
}

pub fn configure_congestion_controller(quality: NetworkQuality) -> quinn::congestion::CubicConfig {
    match quality {
        NetworkQuality::Excellent | NetworkQuality::Good => {
            quinn::congestion::CubicConfig::default()
        }
        NetworkQuality::Fair | NetworkQuality::Poor | NetworkQuality::Unknown => {
            quinn::congestion::CubicConfig::default()
        }
    }
}

pub fn configure_client_for_quality(quality: NetworkQuality) -> Result<ClientConfig> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).map_err(|e| {
            crate::util::error::HarDataError::InvalidConfig(format!(
                "Failed to create QUIC client config: {}",
                e
            ))
        })?,
    ));

    let timeout_config = DynamicTimeout::for_quality(quality);
    let mut transport_config = quinn::TransportConfig::default();

    let idle_timeout = timeout_config.base_connect_timeout * 6;
    if let Ok(timeout) = idle_timeout.try_into() {
        transport_config.max_idle_timeout(Some(timeout));
    }

    transport_config.max_concurrent_bidi_streams(QUIC_MAX_CONCURRENT_STREAMS.into());
    transport_config.max_concurrent_uni_streams(QUIC_MAX_CONCURRENT_STREAMS.into());

    let (stream_window, receive_window, send_window) = match quality {
        NetworkQuality::Excellent => (50 * 1024 * 1024, 200 * 1024 * 1024, 128 * 1024 * 1024),
        NetworkQuality::Good => (32 * 1024 * 1024, 100 * 1024 * 1024, 64 * 1024 * 1024),
        NetworkQuality::Fair => (16 * 1024 * 1024, 32 * 1024 * 1024, 16 * 1024 * 1024),
        NetworkQuality::Poor | NetworkQuality::Unknown => {
            (8 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024 * 1024)
        }
    };

    transport_config.stream_receive_window(quinn::VarInt::from_u32(stream_window as u32));
    if let Ok(window) = quinn::VarInt::from_u64(receive_window as u64) {
        transport_config.receive_window(window);
    }

    if let Ok(window) = quinn::VarInt::from_u64(send_window as u64) {
        transport_config.send_window(window.into_inner());
    }

    let initial_rtt = match quality {
        NetworkQuality::Excellent => Duration::from_millis(20),
        NetworkQuality::Good => Duration::from_millis(50),
        NetworkQuality::Fair => Duration::from_millis(100),
        NetworkQuality::Poor | NetworkQuality::Unknown => Duration::from_millis(200),
    };
    transport_config.initial_rtt(initial_rtt);

    transport_config.keep_alive_interval(Some(Duration::from_secs(10)));

    let datagram_buffer = match quality {
        NetworkQuality::Excellent | NetworkQuality::Good => 64 * 1024 * 1024,
        NetworkQuality::Fair => 32 * 1024 * 1024,
        NetworkQuality::Poor | NetworkQuality::Unknown => 16 * 1024 * 1024,
    };
    transport_config.datagram_receive_buffer_size(Some(datagram_buffer));

    let congestion_config = configure_congestion_controller(quality);
    transport_config.congestion_controller_factory(Arc::new(congestion_config));

    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

#[derive(Debug)]
pub struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}
