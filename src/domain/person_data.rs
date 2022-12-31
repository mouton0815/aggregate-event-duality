use serde::{Serialize, Deserialize};

///
/// Person data as received via ``POST`` requests and stored in
/// [PersonTable](crate::database::person_table::PersonTable).
///
/// Hint: Changes of person data are expressed by
/// [PersonPatch](crate::domain::person_patch::PersonPatch) objects.
///
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonData {
    pub name: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spouse: Option<u32>
}

impl PersonData {
    /// Convenience function that takes &str literals
    pub fn new(name: &str, city: Option<&str>, spouse: Option<u32>) -> Self {
        Self {
            name: String::from(name),
            city: city.map(|l| String::from(l)),
            spouse
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_person1() {
        let person_ref = PersonData::new("Hans", None, Some(2));
        let json_ref = r#"{"name":"Hans","spouse":2}"#;
        serde_and_verify(&person_ref, json_ref);
    }

    #[test]
    pub fn test_person2() {
        let person_ref = PersonData::new("Inge", Some("City"), None);
        let json_ref = r#"{"name":"Inge","city":"City"}"#;
        serde_and_verify(&person_ref, json_ref);
    }
}

