use std::{fs, path::Path};

use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};

const USER_KEY: &str = "RADIXDB_SOAK_HTTP_USER";
const PASSWORD_KEY: &str = "RADIXDB_SOAK_HTTP_PASSWORD";

#[derive(Clone)]
pub struct BasicAuth {
    expected_sha256: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("cannot read HTTP auth file: {0}")]
    Read(#[from] std::io::Error),
    #[error("invalid HTTP auth file: {0}")]
    Invalid(String),
}

impl BasicAuth {
    pub fn load(path: &Path) -> Result<Self, AuthError> {
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(AuthError::Invalid("path is not a regular file".into()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // systemd credentials are exposed read-only as 0440 on hosts that
            // cannot use an id-mapped credential mount. The group is the
            // dedicated service group, so group-read is intentional; group
            // write/execute and every permission for others remain forbidden.
            if metadata.mode() & 0o037 != 0 {
                return Err(AuthError::Invalid(
                    "auth file has unsafe group or other permissions".into(),
                ));
            }
        }
        Self::parse(&fs::read_to_string(path)?)
    }

    pub fn parse(text: &str) -> Result<Self, AuthError> {
        let mut username = None;
        let mut password = None;
        for (index, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| AuthError::Invalid(format!("line {} has no `=`", index + 1)))?;
            let value = value.trim();
            if value.is_empty() || value.len() > 512 {
                return Err(AuthError::Invalid(format!(
                    "line {} has an invalid value length",
                    index + 1
                )));
            }
            if value.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(AuthError::Invalid(format!(
                    "line {} contains a control character",
                    index + 1
                )));
            }
            match key.trim() {
                USER_KEY if username.is_none() => username = Some(value.as_bytes().to_vec()),
                PASSWORD_KEY if password.is_none() => password = Some(value.as_bytes().to_vec()),
                USER_KEY | PASSWORD_KEY => {
                    return Err(AuthError::Invalid(format!(
                        "line {} repeats `{}`",
                        index + 1,
                        key.trim()
                    )))
                }
                other => {
                    return Err(AuthError::Invalid(format!(
                        "line {} contains unknown key `{other}`",
                        index + 1
                    )))
                }
            }
        }
        let username =
            username.ok_or_else(|| AuthError::Invalid(format!("missing `{USER_KEY}`")))?;
        let password =
            password.ok_or_else(|| AuthError::Invalid(format!("missing `{PASSWORD_KEY}`")))?;
        if username.contains(&b':') {
            return Err(AuthError::Invalid(
                "HTTP username cannot contain `:`".into(),
            ));
        }
        let mut expected = Vec::with_capacity(username.len() + password.len() + 1);
        expected.extend_from_slice(&username);
        expected.push(b':');
        expected.extend_from_slice(&password);
        Ok(Self {
            expected_sha256: Sha256::digest(expected).into(),
        })
    }

    pub fn accepts_header(&self, header: Option<&str>) -> bool {
        let Some(encoded) = header.and_then(|value| value.strip_prefix("Basic ")) else {
            return false;
        };
        if encoded.len() > 2048 {
            return false;
        }
        let Ok(decoded) = STANDARD.decode(encoded) else {
            return false;
        };
        let actual: [u8; 32] = Sha256::digest(decoded).into();
        constant_time_eq(&actual, &self.expected_sha256)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_exact_basic_credentials() {
        let auth = BasicAuth::parse(
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=correct horse\n",
        )
        .unwrap();
        let good = format!("Basic {}", STANDARD.encode("observer:correct horse"));
        let bad = format!("Basic {}", STANDARD.encode("observer:wrong"));
        assert!(auth.accepts_header(Some(&good)));
        assert!(!auth.accepts_header(Some(&bad)));
        assert!(!auth.accepts_header(None));
        assert!(!auth.accepts_header(Some("Bearer nope")));
    }

    #[test]
    fn rejects_duplicates_unknown_keys_and_colon_user() {
        assert!(BasicAuth::parse(
            "RADIXDB_SOAK_HTTP_USER=a\nRADIXDB_SOAK_HTTP_USER=b\nRADIXDB_SOAK_HTTP_PASSWORD=c"
        )
        .is_err());
        assert!(BasicAuth::parse(
            "RADIXDB_SOAK_HTTP_USER=a\nOTHER=b\nRADIXDB_SOAK_HTTP_PASSWORD=c"
        )
        .is_err());
        assert!(
            BasicAuth::parse("RADIXDB_SOAK_HTTP_USER=a:b\nRADIXDB_SOAK_HTTP_PASSWORD=c").is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn accepts_systemd_credential_mode_and_rejects_world_readable_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("status-auth");
        fs::write(
            &path,
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=secret\n",
        )
        .unwrap();

        fs::set_permissions(&path, fs::Permissions::from_mode(0o440)).unwrap();
        assert!(BasicAuth::load(&path).is_ok());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
        assert!(BasicAuth::load(&path).is_err());
    }
}
