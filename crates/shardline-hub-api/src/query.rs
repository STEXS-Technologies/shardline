//! Structured contract for an optional isolated dataset query worker.
//!
//! This module deliberately contains no SQL or execution code.  It is the
//! boundary shared by a future sidecar and the Hub API: requests are pinned to
//! an immutable file identity and can only express bounded, allow-listed
//! operations.

use serde::{Deserialize, Serialize};

pub const MAX_COLUMNS: usize = 128;
pub const MAX_PREDICATES: usize = 32;
pub const MAX_LIMIT: u32 = 10_000;
pub const MAX_OFFSET: u64 = 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatasetQueryRequest {
    pub repository: String,
    pub revision: String,
    pub file_sha: String,
    pub config: String,
    pub split: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub predicates: Vec<Predicate>,
    #[serde(default)]
    pub order_by: Vec<OrderTerm>,
    #[serde(default)]
    pub offset: u64,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub aggregates: Vec<Aggregate>,
}

const fn default_limit() -> u32 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Predicate {
    pub column: String,
    pub op: PredicateOp,
    pub value: Scalar,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Scalar {
    Null,
    Bool(bool),
    Integer(i64),
    Float(String),
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderTerm {
    pub column: String,
    #[serde(default)]
    pub descending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Aggregate {
    pub function: AggregateFunction,
    pub column: Option<String>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunction {
    Count,
    Min,
    Max,
    Sum,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValidationError {
    EmptyField(&'static str),
    InvalidIdentifier(&'static str),
    TooMany(&'static str),
    OutOfRange(&'static str),
    InvalidAggregate,
}

impl std::fmt::Display for QueryValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyField(field) => write!(f, "{field} must not be empty"),
            Self::InvalidIdentifier(field) => write!(f, "invalid {field} identifier"),
            Self::TooMany(field) => write!(f, "too many {field}"),
            Self::OutOfRange(field) => write!(f, "{field} is out of range"),
            Self::InvalidAggregate => write!(f, "invalid aggregate"),
        }
    }
}

impl std::error::Error for QueryValidationError {}

impl DatasetQueryRequest {
    pub fn validate(&self) -> Result<(), QueryValidationError> {
        for (field, value) in [
            ("repository", &self.repository),
            ("revision", &self.revision),
            ("file_sha", &self.file_sha),
            ("config", &self.config),
            ("split", &self.split),
        ] {
            if value.is_empty() {
                return Err(QueryValidationError::EmptyField(field));
            }
        }
        let mut repository_parts = self.repository.split('/');
        match (
            repository_parts.next(),
            repository_parts.next(),
            repository_parts.next(),
        ) {
            (Some(owner), Some(name), None) if !owner.is_empty() && !name.is_empty() => {
                validate_identifier(owner, "repository")?;
                validate_identifier(name, "repository")?;
            }
            _ => return Err(QueryValidationError::InvalidIdentifier("repository")),
        }
        validate_identifier(&self.config, "config")?;
        validate_identifier(&self.split, "split")?;
        for (field, value) in [("revision", &self.revision), ("file_sha", &self.file_sha)] {
            if value.len() < 16 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(QueryValidationError::InvalidIdentifier(field));
            }
        }
        if self.columns.len() > MAX_COLUMNS {
            return Err(QueryValidationError::TooMany("columns"));
        }
        if self.predicates.len() > MAX_PREDICATES {
            return Err(QueryValidationError::TooMany("predicates"));
        }
        if self.limit == 0 || self.limit > MAX_LIMIT {
            return Err(QueryValidationError::OutOfRange("limit"));
        }
        if self.offset > MAX_OFFSET {
            return Err(QueryValidationError::OutOfRange("offset"));
        }
        for column in &self.columns {
            validate_identifier(column, "column")?;
        }
        for predicate in &self.predicates {
            validate_identifier(&predicate.column, "predicate column")?;
        }
        for term in &self.order_by {
            validate_identifier(&term.column, "order column")?;
        }
        for aggregate in &self.aggregates {
            if !matches!(aggregate.function, AggregateFunction::Count) && aggregate.column.is_none()
            {
                return Err(QueryValidationError::InvalidAggregate);
            }
            if let Some(column) = &aggregate.column {
                validate_identifier(column, "aggregate column")?;
            }
            if let Some(alias) = &aggregate.alias {
                validate_identifier(alias, "aggregate alias")?;
            }
        }
        Ok(())
    }
}

fn validate_identifier(value: &str, field: &'static str) -> Result<(), QueryValidationError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(QueryValidationError::InvalidIdentifier(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> DatasetQueryRequest {
        DatasetQueryRequest {
            repository: "org/data".into(),
            revision: "a".repeat(40),
            file_sha: "b".repeat(64),
            config: "default".into(),
            split: "train".into(),
            columns: vec!["id".into()],
            predicates: vec![],
            order_by: vec![],
            offset: 0,
            limit: 100,
            aggregates: vec![],
        }
    }

    #[test]
    fn accepts_bounded_revision_pinned_request() {
        assert!(request().validate().is_ok());
    }

    #[test]
    fn rejects_sql_and_path_injection_identifiers() {
        let mut req = request();
        req.columns = vec!["id; DROP TABLE files".into()];
        assert!(matches!(
            req.validate(),
            Err(QueryValidationError::InvalidIdentifier("column"))
        ));
    }

    #[test]
    fn rejects_unbounded_limits_and_invalid_sha() {
        let mut req = request();
        req.limit = MAX_LIMIT + 1;
        assert!(matches!(
            req.validate(),
            Err(QueryValidationError::OutOfRange("limit"))
        ));
        req = request();
        req.file_sha = "not-a-sha".into();
        assert!(matches!(
            req.validate(),
            Err(QueryValidationError::InvalidIdentifier("file_sha"))
        ));
    }

    #[test]
    fn rejects_aggregate_without_column() {
        let mut req = request();
        req.aggregates = vec![Aggregate {
            function: AggregateFunction::Sum,
            column: None,
            alias: None,
        }];
        assert_eq!(req.validate(), Err(QueryValidationError::InvalidAggregate));
    }
}
