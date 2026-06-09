//! Lazy error context helper trait for RemoteFS.
//!
//! Provides a consistent mechanism to append execution context to typed errors,
//! mimicking the behavior of `anyhow::Context` without losing error types.

/// A trait for error types that can be wrapped in an operation context.
pub trait ResultContextError: Sized {
    /// Wraps the error with the provided operation description.
    fn with_context(self, operation: String) -> Self;
}

/// Extension trait for `Result` to lazily attach context to errors.
pub trait ResultContext<T, E> {
    /// Attach a static string context to a failing `Result`.
    fn context(self, operation: &'static str) -> Result<T, E>;

    /// Attach a lazily-evaluated string context to a failing `Result`.
    ///
    /// The closure is only executed if the result is an `Err`.
    fn with_context<F>(self, operation: F) -> Result<T, E>
    where
        F: FnOnce() -> String;
}

impl<T, E> ResultContext<T, E> for Result<T, E>
where
    E: ResultContextError,
{
    fn context(self, operation: &'static str) -> Result<T, E> {
        self.map_err(|source| source.with_context(operation.to_owned()))
    }

    fn with_context<F>(self, operation: F) -> Result<T, E>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|source| source.with_context(operation()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Base,
        Context {
            operation: String,
            source: Box<TestError>,
        },
    }

    impl ResultContextError for TestError {
        fn with_context(self, operation: String) -> Self {
            TestError::Context {
                operation,
                source: Box::new(self),
            }
        }
    }

    #[test]
    fn test_context_static_message() {
        let result: Result<(), TestError> = Err(TestError::Base);
        let wrapped = result.context("static message").unwrap_err();
        assert_eq!(
            wrapped,
            TestError::Context {
                operation: "static message".to_owned(),
                source: Box::new(TestError::Base)
            }
        );
    }

    #[test]
    fn test_with_context_lazy_message() {
        let result: Result<(), TestError> = Err(TestError::Base);
        let wrapped = result
            .with_context(|| format!("lazy message {}", 42))
            .unwrap_err();
        assert_eq!(
            wrapped,
            TestError::Context {
                operation: "lazy message 42".to_owned(),
                source: Box::new(TestError::Base)
            }
        );
    }

    #[test]
    fn test_with_context_does_not_evaluate_on_ok() {
        let called = Cell::new(false);
        let result: Result<(), TestError> = Ok(());
        result
            .with_context(|| {
                called.set(true);
                "should not be evaluated".to_owned()
            })
            .unwrap();
        assert!(!called.get());
    }
}
