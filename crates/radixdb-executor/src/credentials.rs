//! Shared password-verifier primitives for catalog and server authentication.

use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2, Params,
};
use radixdb_core::{Error, Result};

pub const MAX_PASSWORD_BYTES: usize = 1024;
const MIN_MEMORY_COST_KIB: u32 = Params::DEFAULT_M_COST;
const MAX_MEMORY_COST_KIB: u32 = 256 * 1024;
const MIN_TIME_COST: u32 = Params::DEFAULT_T_COST;
const MAX_TIME_COST: u32 = 10;
const MAX_PARALLELISM: u32 = 16;

/// Derive a salted Argon2id PHC verifier without retaining the plaintext.
pub fn hash_password_verifier(password: &str) -> Result<String> {
    validate_password_length(password)?;
    let salt_seed = uuid::Uuid::now_v7();
    let salt = SaltString::encode_b64(salt_seed.as_bytes())
        .map_err(|_| Error::internal("failed to encode credential salt"))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| Error::internal("failed to derive credential verifier"))
}

/// Validate that a configured verifier is a syntactically valid Argon2id PHC string.
pub fn validate_password_verifier(encoded: &str) -> Result<()> {
    let parsed = PasswordHash::new(encoded)
        .map_err(|_| Error::invalid_argument("password verifier must be a valid PHC string"))?;
    if parsed.algorithm.as_str() != "argon2id" {
        return Err(Error::invalid_argument(
            "password verifier must use the argon2id algorithm",
        ));
    }
    if parsed.version != Some(19) || parsed.salt.is_none() || parsed.hash.is_none() {
        return Err(Error::invalid_argument(
            "password verifier must contain Argon2 version 19, salt and hash",
        ));
    }
    let params = Params::try_from(&parsed)
        .map_err(|_| Error::invalid_argument("password verifier parameters are invalid"))?;
    let output_len = params.output_len().unwrap_or(Params::DEFAULT_OUTPUT_LEN);
    if !(MIN_MEMORY_COST_KIB..=MAX_MEMORY_COST_KIB).contains(&params.m_cost())
        || !(MIN_TIME_COST..=MAX_TIME_COST).contains(&params.t_cost())
        || !(Params::MIN_P_COST..=MAX_PARALLELISM).contains(&params.p_cost())
        || !(Params::DEFAULT_OUTPUT_LEN..=64).contains(&output_len)
    {
        return Err(Error::invalid_argument(
            "password verifier parameters are outside the supported security bounds",
        ));
    }
    Ok(())
}

/// Verify a password without disclosing why a verifier did not match.
pub fn verify_password_verifier(encoded: &str, password: &str) -> bool {
    if password.is_empty() || password.len() > MAX_PASSWORD_BYTES {
        return false;
    }
    if validate_password_verifier(encoded).is_err() {
        return false;
    }
    let parsed = PasswordHash::new(encoded).expect("validated password verifier must parse");
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

fn validate_password_length(password: &str) -> Result<()> {
    if password.is_empty() || password.len() > MAX_PASSWORD_BYTES {
        return Err(Error::invalid_argument(
            "password length must be in 1..=1024 bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_verifier_is_argon2id_and_matches_only_its_password() {
        let encoded = hash_password_verifier("correct horse battery staple").unwrap();
        assert!(encoded.starts_with("$argon2id$"));
        assert_eq!(validate_password_verifier(&encoded), Ok(()));
        assert!(verify_password_verifier(
            &encoded,
            "correct horse battery staple"
        ));
        assert!(!verify_password_verifier(&encoded, "wrong"));
    }

    #[test]
    fn malformed_and_non_argon2id_verifiers_are_rejected() {
        assert!(validate_password_verifier("not-a-phc-string").is_err());
        assert!(validate_password_verifier("$argon2i$v=19$m=4096,t=3,p=1$c2FsdA$YWJj").is_err());
        assert!(validate_password_verifier("$argon2id$v=19$m=19456,t=2,p=1$c2FsdA").is_err());
        assert!(validate_password_verifier(
            "$argon2id$v=19$m=4294967295,t=2,p=1$c2FsdA$YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo"
        )
        .is_err());
        assert!(!verify_password_verifier("not-a-phc-string", "secret"));
    }

    #[test]
    fn password_length_is_bounded() {
        assert!(hash_password_verifier("").is_err());
        assert!(hash_password_verifier(&"x".repeat(MAX_PASSWORD_BYTES + 1)).is_err());
    }
}
