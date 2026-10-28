//! Structured engine failures. Converting legacy text never invents a cause.
use std::{fmt, ops::Deref};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCause {
    DecimalPrecisionOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    message: String,
    cause: Option<ErrorCause>,
}
impl Error {
    pub fn as_str(&self) -> &str {
        &self.message
    }
    pub(crate) fn context(mut self, suffix: impl std::fmt::Display) -> Self {
        self.message = format!("{}{}", self.message, suffix);
        self
    }
    pub fn cause(&self) -> Option<ErrorCause> {
        self.cause
    }
    pub(crate) fn decimal_precision_overflow() -> Self {
        Self {
            message: "decimal cast precision overflow".into(),
            cause: Some(ErrorCause::DecimalPrecisionOverflow),
        }
    }
}
impl From<String> for Error {
    fn from(message: String) -> Self {
        Self {
            message,
            cause: None,
        }
    }
}
impl From<&str> for Error {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl From<Error> for String {
    fn from(error: Error) -> Self {
        error.message
    }
}
impl Deref for Error {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for Error {}
impl serde::Serialize for Error {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.message)
    }
}
impl PartialEq<&str> for Error {
    fn eq(&self, other: &&str) -> bool {
        self.message == *other
    }
}
impl PartialEq<String> for Error {
    fn eq(&self, other: &String) -> bool {
        self.message == *other
    }
}

impl PartialEq<str> for Error {
    fn eq(&self, other: &str) -> bool {
        self.message == other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_text_cannot_assert_a_cause() {
        let typed = Error::decimal_precision_overflow();
        let text = Error::from(typed.to_string());
        assert_eq!(typed.cause(), Some(ErrorCause::DecimalPrecisionOverflow));
        assert_eq!(text.cause(), None);
        assert_eq!(
            typed.clone().context("; retry context").cause(),
            typed.cause()
        );
        assert_eq!(
            serde_json::to_string(&typed).unwrap(),
            "\"decimal cast precision overflow\""
        );
    }
}
