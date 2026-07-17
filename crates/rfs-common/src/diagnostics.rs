//! Stable diagnostic metadata and source-chain formatting at process boundaries.

use std::error::Error;

use serde::Serialize;

/// One safe structured detail attached to a diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiagnosticDetail {
    /// Stable field name.
    pub key: &'static str,
    /// Presentation-safe field value.
    pub value: String,
}

/// Stable diagnostic identity implemented by crate-owned structured errors.
pub trait Diagnostic {
    /// Returns the stable machine-readable error code.
    fn code(&self) -> &'static str;

    /// Returns safe structured fields suitable for command or control output.
    fn details(&self) -> Vec<DiagnosticDetail> {
        Vec::new()
    }
}

/// Formats an error and its sources without discarding the original chain.
pub fn format_source_chain(error: &(dyn Error + 'static)) -> String {
    let mut output = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        output.push_str(": ");
        output.push_str(&error.to_string());
        source = error.source();
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_complete_source_chain() {
        let source = std::io::Error::other("disk unavailable");
        let error = source_chain_fixture(source);
        assert_eq!(
            format_source_chain(&error),
            "write session metadata: disk unavailable"
        );
    }

    fn source_chain_fixture(source: std::io::Error) -> FixtureError {
        FixtureError { source }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("write session metadata")]
    struct FixtureError {
        #[source]
        source: std::io::Error,
    }
}
