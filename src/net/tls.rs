// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::config::ServerConfig;
use crate::persist::aof::AofWriter;
use crate::replication::backlog::ReplBacklog;
use crate::net::connection;

/// Loads a PEM certificate chain and private key into a `rustls`
/// server config suitable for `tokio_rustls::TlsAcceptor`.
pub fn load_tls_config(cert_path: &Path, key_path: &Path) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS cert file {:?}: {}", cert_path, e))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse TLS cert file {:?}: {}", cert_path, e))?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {:?}", cert_path);
    }

    let key_file = std::fs::File::open(key_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS key file {:?}: {}", key_path, e))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| anyhow::anyhow!("failed to parse TLS key file {:?}: {}", key_path, e))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {:?}", key_path))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("invalid TLS cert/key pair: {}", e))?;
    Ok(Arc::new(config))
}

/// Runs a TLS-terminated accept loop on `listener`, handing each
/// completed handshake to the same command pipeline as the plaintext
/// port. Runs until the listener errors or the process is asked to stop.
pub async fn accept_loop(
    listener: TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    store: Arc<Store>,
    cmd_table: Arc<CommandTable>,
    config: ServerConfig,
    aof_writer: Option<Arc<AofWriter>>,
    repl_backlog: Option<Arc<std::sync::Mutex<ReplBacklog>>>,
    replid: String,
) -> anyhow::Result<()> {
    let acceptor = TlsAcceptor::from(tls_config);
    loop {
        let (socket, addr) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let store = store.clone();
        let cmd_table = cmd_table.clone();
        let config = config.clone();
        let aof_writer = aof_writer.clone();
        let repl_backlog = repl_backlog.clone();
        let replid = replid.clone();
        tokio::spawn(async move {
            match acceptor.accept(socket).await {
                Ok(tls_stream) => {
                    if let Err(e) = connection::handle(tls_stream, store, cmd_table, config, aof_writer, repl_backlog, replid).await {
                        tracing::warn!("TLS connection error from {}: {}", addr, e);
                    }
                }
                Err(e) => tracing::warn!("TLS handshake failed from {}: {}", addr, e),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Self-signed test cert/key for localhost, valid one year from
    // generation. Test-only — never used for anything but this suite.
    const TEST_CERT: &str = "-----BEGIN CERTIFICATE-----\nMIIDCTCCAfGgAwIBAgIUaKQtzb4t5ZrldEcPL3Tzf0Lyz3swDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcwMjE5Mjc0N1oXDTI3MDcw\nMjE5Mjc0N1owFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\nAAOCAQ8AMIIBCgKCAQEApN5QQTb2cA2dth9I8QPrlu/PVkPZZWZPyH6nwpP/jHw/\naxzc9vijg1wKO2AzuGPJ8oeE+IkRfheh/0OVWK6hgCA1zbW1uaGHJDoqlI1M7vzd\nUfs/96w8xaqWxKERTZ2Ob349oZ3nsw9BRMXte9kOM406459lRTpw9zxHxSzmfvNP\nrfd2K/K22FwDYW8zFmbpkAAB0lLuu4jbs1PbGTj25MZ6cQvH7ZlyYLohdcFLzp6Y\nQq3YsVZ9ABWZFWgqa5VTCNC2aXeRTGhgBh+EIUn5WJ8G0QiWoGwkZC5t/52wZ1v9\ncCaSnG8SHooOKoTXCZkv6hdLncCY7MrgWPPJKvPW7wIDAQABo1MwUTAdBgNVHQ4E\nFgQU+EhiTCWobfhjnYrZN2nw3TrzpHAwHwYDVR0jBBgwFoAU+EhiTCWobfhjnYrZ\nN2nw3TrzpHAwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEApMkO\n1EpbxJW7MFQTH67CzimvBo6vUjkOG8+qkgE7KPr/wBtbZPlYaCp1V9Lr5NJ8gyFh\n85eTwWWZwNdNJaLdVhztjzLyxo0WmHMMvINDDxyOEPsyzg3YLImif4/uptE7Rw23\n5S+dfRtOGhmkUN5suGyP6KYLk2LxXHhGh2L4mg+7kL2eMd8gRHJv+DKs65pdAJ/N\ntlZ/KOFjAgGxZ5Bc4cZWcbqD/chumCq+/kST8C04rPG02w8ebsXb1IH6Q5Kichf4\neLQEhHNS6FCcikNs4r/ClPSX2ccHO2wSCw6srUzwqmN1njT69XQJjZ5ws49TI0o/\ng9Bf25NFGWIi7HpMmQ==\n-----END CERTIFICATE-----\n";
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQCk3lBBNvZwDZ22\nH0jxA+uW789WQ9llZk/IfqfCk/+MfD9rHNz2+KODXAo7YDO4Y8nyh4T4iRF+F6H/\nQ5VYrqGAIDXNtbW5oYckOiqUjUzu/N1R+z/3rDzFqpbEoRFNnY5vfj2hneezD0FE\nxe172Q4zjTrjn2VFOnD3PEfFLOZ+80+t93Yr8rbYXANhbzMWZumQAAHSUu67iNuz\nU9sZOPbkxnpxC8ftmXJguiF1wUvOnphCrdixVn0AFZkVaCprlVMI0LZpd5FMaGAG\nH4QhSflYnwbRCJagbCRkLm3/nbBnW/1wJpKcbxIeig4qhNcJmS/qF0udwJjsyuBY\n88kq89bvAgMBAAECgf8TYggAAMED4Wyy3Q/TDJOZlpYkzIefDAhip7QnfK0iU8An\nsFjSQw/xdcTlfwctG3J4c8jIqAzUsR+oOl7yxFMBw/S9aZEyNGLaKkdGf1RcOnoi\n5EGtdghdjRVnIJb3iBiR7QdEfUofLYeFmOqnGNvJN56evKyODoaQstgJ9KAvPh6n\n6HHz1ZRh2v7ghrKC6Y9Db2kX52tLD7F1twgGHpS0+S3KlZ4XO7jZD75CitAB4TBX\n7IPZv1A7pIJkNGTJRQPkRj9cGB4q/OAlDDrgFS26Q+kU3X/t8xezWH0qom9oR0u8\n5eMAkNEsTUKdc3RiqfkyD8EqJtWObZBwXkPSMQECgYEA2BAZqqeQi+Nsvrsx8faZ\nePPLv3UUcjek1Fjx1k8ZY6A+ABhF4LdfD4M2mH/u+EqsnogpXCJJGzn+7tieWaQV\nOlc9gB1vLhGSOEaf+ANGTEt+r4geMFGQurpl8585SdlmE7SwcsNeqMC5dCyfnekM\nThquWCdlX/Nptnbwcm1cx48CgYEAw1e8qjwtUL6dJTA7dYcWvk+Run3VlnUqEdRU\nqpAKT8ZZeSFxu37jOcfQD27Obb3mBGHrymxkAcxK1v5D5u7/38uv6OAQJOsoEjNo\nWrFV6r8HcwNecoju0R1j+7C8ZkMkwA1G649Sjc17C3CVLhg28GKLjsA7oWYK25id\n2IHlSqECgYA7joyUpuXIOaNTG+STjucVGRazqsE9DquHwRDAg0M7XANbIVW5sLIq\nY3/cH3+uv16/wEauV+EQ+TaVfe6ARSN41m1kcDiiWUOV8ZnM0pJBG5pLJlkz9nfP\nkOvjcKNpAN0LV4Y/zCy+lYlJRbel3oR+zwn50Lo37a/ZFQYIdK7bbwKBgBw/Yrvn\nSdJETisjh2WebE6G6RbjvXAtbzD22Gt1utgAYc3fZTfsyGUBnPeuWVGLRWja1CMI\ne4m7BhOC30TUyNGO/dgaFpuQGdJP9sYuoLL6ftRF53F+lbJNorixvPy4tubCxL+p\nkRGKZkGoPRpWTBOE3JN+/uB/BhDtR94YIpUhAoGAQme18NkvWTGrm0ujR87ELTjV\ntJBae1Ke7GWL30XloF014kO3h3w7p6cTAeHepU/KCMiUmA6pXSM3186/CHQEi23c\nOyf643XTnK1JOjargk/3vlZE8PyiFs3TV2b2qZnYlnV3uFrnkNtf5TwaxxMy5/RQ\nz5KCkjduF7fMLZpYFKo=\n-----END PRIVATE KEY-----\n";

    fn write_fixture(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, TEST_CERT).unwrap();
        std::fs::write(&key_path, TEST_KEY).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn test_load_valid_cert_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_fixture(dir.path());
        let config = load_tls_config(&cert_path, &key_path);
        assert!(config.is_ok(), "expected valid cert/key to load: {:?}", config.err());
    }

    #[test]
    fn test_load_missing_cert_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (_, key_path) = write_fixture(dir.path());
        let missing = dir.path().join("nope.pem");
        let result = load_tls_config(&missing, &key_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_missing_key_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, _) = write_fixture(dir.path());
        let missing = dir.path().join("nope.pem");
        let result = load_tls_config(&cert_path, &missing);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_malformed_cert_errors() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, "not a cert").unwrap();
        std::fs::write(&key_path, TEST_KEY).unwrap();
        let result = load_tls_config(&cert_path, &key_path);
        assert!(result.is_err());
    }
}
