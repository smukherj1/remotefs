use serde::Serialize;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

use crate::shared::reapi::remote_execution;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Digest {
    hash: String,
    size_bytes: i64,
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DigestError {
    #[error("Invalid digest format. Expected 'sha256:<64-lowercase-hex>/<decimal-size-bytes>'")]
    InvalidFormat,
    #[error("Unsupported hash function. Only 'sha256' is supported")]
    UnsupportedHash,
    #[error("Invalid hash character or length. Expected 64 lowercase hex characters")]
    InvalidHash,
    #[error("Invalid size value: {0}")]
    InvalidSize(String),
    #[error("Digest size mismatch: expected {expected} bytes, got {actual} bytes")]
    SizeMismatch { expected: i64, actual: i64 },
    #[error("Digest hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

impl Digest {
    pub fn new(hash: String, size_bytes: i64) -> Result<Self, DigestError> {
        if hash.len() != 64 {
            return Err(DigestError::InvalidHash);
        }
        for c in hash.chars() {
            if !c.is_ascii_hexdigit() || (c.is_ascii_alphabetic() && !c.is_ascii_lowercase()) {
                return Err(DigestError::InvalidHash);
            }
        }
        if size_bytes < 0 {
            return Err(DigestError::InvalidSize(size_bytes.to_string()));
        }
        Ok(Digest { hash, size_bytes })
    }

    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn size_bytes(&self) -> i64 {
        self.size_bytes
    }

    pub fn for_bytes(bytes: &[u8]) -> Self {
        use sha2::{Digest as ShaDigest, Sha256};

        let hash = hex::encode(Sha256::digest(bytes));
        Digest {
            hash,
            size_bytes: bytes.len() as i64,
        }
    }

    pub fn verify_bytes(&self, bytes: &[u8]) -> Result<(), DigestError> {
        let actual = Self::for_bytes(bytes);
        if actual.size_bytes != self.size_bytes {
            return Err(DigestError::SizeMismatch {
                expected: self.size_bytes,
                actual: actual.size_bytes,
            });
        }
        if actual.hash != self.hash {
            return Err(DigestError::HashMismatch {
                expected: self.hash.clone(),
                actual: actual.hash,
            });
        }
        Ok(())
    }

    pub fn to_reapi(&self) -> remote_execution::Digest {
        remote_execution::Digest {
            hash: self.hash.clone(),
            size_bytes: self.size_bytes,
        }
    }

    pub fn from_reapi(digest: &remote_execution::Digest) -> Result<Self, DigestError> {
        Self::new(digest.hash.clone(), digest.size_bytes)
    }
}

impl TryFrom<remote_execution::Digest> for Digest {
    type Error = DigestError;

    fn try_from(value: remote_execution::Digest) -> Result<Self, Self::Error> {
        Self::new(value.hash, value.size_bytes)
    }
}

impl From<&Digest> for remote_execution::Digest {
    fn from(value: &Digest) -> Self {
        value.to_reapi()
    }
}

impl FromStr for Digest {
    type Err = DigestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some(stripped) = s.strip_prefix("sha256:") else {
            return Err(DigestError::UnsupportedHash);
        };
        let Some((hash_part, size_part)) = stripped.split_once('/') else {
            return Err(DigestError::InvalidFormat);
        };
        if hash_part.len() != 64 {
            return Err(DigestError::InvalidHash);
        }
        for c in hash_part.chars() {
            if !c.is_ascii_hexdigit() || (c.is_ascii_alphabetic() && !c.is_ascii_lowercase()) {
                return Err(DigestError::InvalidHash);
            }
        }
        if size_part.is_empty() {
            return Err(DigestError::InvalidFormat);
        }
        if !size_part.chars().all(|c| c.is_ascii_digit()) {
            return Err(DigestError::InvalidSize(size_part.to_string()));
        }
        let size_bytes = size_part
            .parse::<i64>()
            .map_err(|_| DigestError::InvalidSize(size_part.to_string()))?;
        if size_bytes < 0 {
            return Err(DigestError::InvalidSize(size_part.to_string()));
        }
        Ok(Digest {
            hash: hash_part.to_string(),
            size_bytes,
        })
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sha256:{}/{}", self.hash, self.size_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_digest() {
        let s = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0";
        let d: Digest = s.parse().unwrap();
        assert_eq!(
            d.hash(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(d.size_bytes(), 0);
        assert_eq!(d.to_string(), s);
    }

    #[test]
    fn test_invalid_hash_function() {
        let s = "md5:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0";
        assert_eq!(s.parse::<Digest>(), Err(DigestError::UnsupportedHash));
    }

    #[test]
    fn test_missing_size() {
        let s = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(s.parse::<Digest>(), Err(DigestError::InvalidFormat));
    }

    #[test]
    fn test_uppercase_hash() {
        let s = "sha256:E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855/0";
        assert_eq!(s.parse::<Digest>(), Err(DigestError::InvalidHash));
    }

    #[test]
    fn test_invalid_hash_characters() {
        let s = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85g/0";
        assert_eq!(s.parse::<Digest>(), Err(DigestError::InvalidHash));
    }

    #[test]
    fn test_invalid_hash_length() {
        let s = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85/0"; // 62 chars
        assert_eq!(s.parse::<Digest>(), Err(DigestError::InvalidHash));
    }

    #[test]
    fn test_negative_size() {
        let s = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/-10";
        assert!(matches!(
            s.parse::<Digest>(),
            Err(DigestError::InvalidSize(_))
        ));
    }

    #[test]
    fn test_size_overflow() {
        let s = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/999999999999999999999999999999";
        assert!(matches!(
            s.parse::<Digest>(),
            Err(DigestError::InvalidSize(_))
        ));
    }

    #[test]
    fn test_digest_for_bytes_formats_canonical_sha256() {
        let digest = Digest::for_bytes(b"");
        assert_eq!(
            digest.to_string(),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0"
        );
    }
}
