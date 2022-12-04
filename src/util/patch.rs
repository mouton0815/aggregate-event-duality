use rusqlite::ToSql;
use rusqlite::types::ToSqlOutput;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum Patch<T> {
    Value(T), // Set the aggregate value to the value of T
    Null,     // Set the aggregate value to null
    Absent    // Do not change the aggregate value
}

impl<T> Patch<T> {

    /// Computes a patch between two options "old" and "new". The result is
    /// * ```Patch::Absent``` if "new" is equal to "old"
    /// * ```Patch::Null``` if "old" contains a value but "new" does not
    /// * ```Patch::Value(new.unwrap())``` if "new" contains a value but "old" does not,
    /// or if both have different values
    ///
    pub fn of(old: &Option<T>, new: &Option<T>) -> Self where T: Clone + PartialEq {
        if old == new {
            Patch::Absent
        } else if old.is_some() && new.is_none() {
            Patch::Null
        } else {
            Patch::Value(new.as_ref().unwrap().clone())
        }
    }

    pub const fn is_value(&self) -> bool {
        matches!(*self, Patch::Value(_))
    }

    pub const fn is_null(&self) -> bool {
        matches!(*self, Patch::Null)
    }

    pub const fn is_absent(&self) -> bool {
        matches!(*self, Patch::Absent)
    }

    pub const fn as_ref(&self) -> Patch<&T> {
        match *self {
            Patch::Value(ref x) => Patch::Value(x),
            Patch::Absent => Patch::Absent,
            Patch::Null => Patch::Null
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            Patch::Value(val) => val,
            Patch::Absent => panic!("called `Patch::unwrap()` on an `Absent` value"),
            Patch::Null => panic!("called `Patch::unwrap()` on a `Null` value")
        }
    }

    pub fn map<U, F>(self, f: F) -> Patch<U> where F: FnOnce(T) -> U {
        match self {
            Patch::Value(x) => Patch::Value(f(x)),
            Patch::Absent => Patch::Absent,
            Patch::Null => Patch::Null
        }
    }
}

//
// Implement traits
//

// https://stackoverflow.com/a/44332837
impl<T> Default for Patch<T> {
    fn default() -> Self {
        Patch::Absent
    }
}

impl<T> From<Option<T>> for Patch<T> {
    fn from(opt: Option<T>) -> Patch<T> {
        match opt {
            Some(v) => Patch::Value(v),
            None => Patch::Null,
        }
    }
}

impl<'de, T> Deserialize<'de> for Patch<T> where T: Deserialize<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        Option::deserialize(deserializer).map(Into::into)
    }
}

// See https://serde.rs/impl-serialize.html
impl<T> Serialize for Patch<T> where T: Serialize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        match self {
            Patch::Value(t) => serializer.serialize_some(t),
            Patch::Null => serializer.serialize_none(),
            Patch::Absent => serializer.serialize_none(),
        }
    }
}

impl<T: ToSql> ToSql for Patch<T> {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match *self {
            Patch::Value(ref t) => t.to_sql(),
            Patch::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Patch::Absent => Ok(ToSqlOutput::from(rusqlite::types::Null)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::patch::Patch;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct Record {
        #[serde(default)]
        #[serde(skip_serializing_if = "Patch::is_absent")]
        a: Patch<String>,

        #[serde(default)]
        #[serde(skip_serializing_if = "Patch::is_absent")]
        b: Patch<u32>,

        #[serde(default)]
        #[serde(skip_serializing_if = "Patch::is_absent")]
        c: Patch<Vec<i32>>
    }

    #[test]
    pub fn test_of_none_none() {
        let t : Patch<usize> = Patch::of(&None, &None);
        assert_eq!(t, Patch::Absent);
    }

    #[test]
    pub fn test_of_none_some() {
        let t = Patch::of(&None, &Some("foo"));
        assert_eq!(t, Patch::Value("foo"));
    }

    #[test]
    pub fn test_of_some_none() {
        let t = Patch::of(&Some("foo"), &None);
        assert_eq!(t, Patch::Null);
    }

    #[test]
    pub fn test_of_equal() {
        let t = Patch::of(&Some("foo"), &Some("foo"));
        assert_eq!(t, Patch::Absent);
    }

    #[test]
    pub fn test_of_differ() {
        let t = Patch::of(&Some("foo"), &Some("bar"));
        assert_eq!(t, Patch::Value("bar"));
    }

    #[test]
    pub fn test_unwrap() {
        let t = Patch::Value(String::from("123"));
        let r : String = t.unwrap();
        assert_eq!(r, "123");
    }

    #[test]
    pub fn test_as_ref() {
        let t = Patch::Value(String::from("123"));
        let r : Patch<&String> = t.as_ref();
        assert_eq!(r.unwrap(), "123");
    }

    #[test]
    pub fn test_map() {
        let t : Patch<&str> = Patch::Value("123");
        let r : Patch<String> = t.map(|s| String::from(s));
        assert_eq!(r.unwrap(), "123");
    }

    #[test]
    pub fn test_from_some() {
        let t : Patch<u32> = Patch::from(Some(123));
        assert!(t.is_value());
        assert_eq!(t.unwrap(), 123);
    }

    #[test]
    pub fn test_from_none() {
        let t : Patch<u32> = Patch::from(None);
        assert!(t.is_null());
    }

    #[test]
    pub fn test_serde_value() {
        let t = Patch::Value(String::from("123"));
        assert!(t.is_value());
        assert!(!t.is_null());
        assert!(!t.is_absent());

        let json = serde_json::to_string(&t);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(r#""123""#));
    }

    #[test]
    pub fn test_serde_null() {
        let t: Patch<u32> = Patch::Null;
        assert!(!t.is_value());
        assert!(t.is_null());
        assert!(!t.is_absent());

        let json = serde_json::to_string(&t);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from("null"));
    }

    #[test]
    pub fn test_serde_absent() {
        let t: Patch<u32> = Patch::Absent;
        assert!(!t.is_value());
        assert!(!t.is_null());
        assert!(t.is_absent());

        let json = serde_json::to_string(&t);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from("null"));
    }

    #[test]
    pub fn test_serde_record_value() {
        let record_ref = Record{
            a: Patch::Value(String::from("Foo")),
            b: Patch::Value(123),
            c: Patch::Value(vec![3,-5, 7])
        };
        let json_ref = r#"{"a":"Foo","b":123,"c":[3,-5,7]}"#;
        serde_and_verify(&record_ref, json_ref);
    }

    #[test]
    pub fn test_serialize_record_null() {
        let record_ref = Record{
            a: Patch::Null,
            b: Patch::Null,
            c: Patch::Null
        };
        let json_ref = r#"{"a":null,"b":null,"c":null}"#;
        serde_and_verify(&record_ref, json_ref);
    }

    #[test]
    pub fn test_serialize_record_absent() {
        let record_ref = Record{
            a: Patch::Absent,
            b: Patch::Absent,
            c: Patch::Absent
        };
        let json_ref = r#"{}"#;
        serde_and_verify(&record_ref, json_ref);
    }

    fn serde_and_verify(record_ref: &Record, json_ref: &str) {
        let json = serde_json::to_string(&record_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let record : Result<Record, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(record.is_ok());
        assert_eq!(record.unwrap(), *record_ref);
    }
}