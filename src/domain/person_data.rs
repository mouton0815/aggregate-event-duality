use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonData {
    pub name: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spouse_id: Option<u32>
}

impl PersonData {
    /// Convenience function that takes &str literals
    pub fn new(name: &str, location: Option<&str>, spouse_id: Option<u32>) -> PersonData {
        PersonData {
            name: String::from(name),
            location: location.map(|l| String::from(l)),
            spouse_id
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;

    #[test]
    pub fn test_person1() {
        let person_ref = PersonData::new("Hans", None, Some(2));
        let json_ref = r#"{"name":"Hans","spouseId":2}"#;
        serde_and_verify(&person_ref, json_ref);
    }

    #[test]
    pub fn test_person2() {
        let person_ref = PersonData::new("Inge", Some("City"), None);
        let json_ref = r#"{"name":"Inge","location":"City"}"#;
        serde_and_verify(&person_ref, json_ref);
    }

    fn serde_and_verify(person_ref: &PersonData, json_ref: &str) {
        // 1. Serialize person_ref and string-compare it to json_ref
        let json = serde_json::to_string(&person_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with person_ref
        let person: Result<PersonData, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person.is_ok());
        assert_eq!(person.unwrap(), *person_ref);
    }
}

