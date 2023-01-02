use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;
use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use serde::{Serialize, Deserialize};

///
/// A typed id for person records, instead of using u64 in interfaces.
/// Might be a little bit overengineered :)
///
#[derive(Clone, Copy, Hash, Serialize, Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PersonId(u64);

impl From<u64> for PersonId {
    fn from(value: u64) -> Self {
        PersonId { 0: value }
    }
}

impl FromStr for PersonId {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        u64::from_str(s).map(|i| Self{ 0: i })
    }
}

// This also implements to_string()
impl fmt::Display for PersonId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// From https://docs.rs/rusqlite/0.10.2/rusqlite/types/trait.FromSql.html:
// "Note that FromSql and ToSql are defined for most integral types, but not u64 or usize"
// Also note that the conversions below may fail for very large u64 numbers, but we ignore
// this for this prototype.
impl ToSql for PersonId {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for PersonId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => Ok(Self{ 0: i as u64 }),
            _ => Err(FromSqlError::InvalidType)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::str::FromStr;
    use rusqlite::ToSql;
    use rusqlite::types::{FromSql, ToSqlOutput, Value, ValueRef};
    use serde::{Serialize, Deserialize};
    use crate::domain::person_id::PersonId;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct Record {
        key: PersonId
    }

    #[test]
    fn test_serde_key() {
        let data = PersonId::from(123);
        serde_and_verify(&data, r#"123"#);
    }

    #[test]
    fn test_serde_record() {
        let data = Record{ key: PersonId::from(123) };
        serde_and_verify(&data, r#"{"key":123}"#);
    }

    #[test]
    fn test_serde_hash_map() {
        let mut data = HashMap::<PersonId, u32>::new();
        data.insert(PersonId::from(123), 456);
        serde_and_verify(&data, r#"{"123":456}"#); // Note: Keys in JSON are always strings
    }

    #[test]
    fn test_serde_tree_map() {
        let mut data = BTreeMap::<PersonId, String>::new();
        data.insert(PersonId::from(123), String::from("dummy"));
        serde_and_verify(&data, r#"{"123":"dummy"}"#);
    }

    #[test]
    fn test_from_str_ok() {
        let result = PersonId::from_str("123");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PersonId::from(123));
    }

    #[test]
    fn test_from_str_err() {
        let result = PersonId::from_str("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_display() {
        let result = PersonId::from(123).to_string();
        assert_eq!(result, "123");
    }

    #[test]
    fn test_to_sql() {
        let id = PersonId::from(123);
        let sql = id.to_sql();
        assert!(sql.is_ok());
        match sql.unwrap() {
            ToSqlOutput::Owned(value) => assert_eq!(value, Value::Integer(123)),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_from_sql() {
        let result = PersonId::column_result(ValueRef::Integer(123));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PersonId::from(123));
    }
}