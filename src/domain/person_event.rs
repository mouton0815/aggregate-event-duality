use serde::{Serialize, Deserialize};
use crate::util::patch::Patch;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonData {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>, // Name can be updated or left as is, but not deleted

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub location: Patch<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub spouse_id: Patch<u32>
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonEvent {
    pub person_id: u32,

    #[serde(default)]
    pub data: Patch<PersonData>
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use serde::{Deserialize, Serialize};
    use crate::util::patch::Patch;
    use crate::domain::person_event::{PersonEvent, PersonData};

    #[test]
    pub fn test_serde_person_event_create() {
        let person_ref = PersonEvent {
            person_id: 10,
            data: Patch::Value(PersonData {
                name: Some(String::from("Hans")),
                location: Patch::Value(String::from("Nowhere")),
                spouse_id: Patch::Value(20)
            })
        };

        let json_ref = r#"{"personId":10,"data":{"name":"Hans","location":"Nowhere","spouseId":20}}"#;

        serde_and_verify(&person_ref, json_ref);
    }

    #[test]
    pub fn test_serde_person_event_update() {
        let person_ref = PersonEvent {
            person_id: 10,
            data: Patch::Value(PersonData {
                name: None,
                location: Patch::Null,
                spouse_id: Patch::Null
            })
        };

        let json_ref = r#"{"personId":10,"data":{"location":null,"spouseId":null}}"#;

        serde_and_verify(&person_ref, json_ref);
    }

    #[test]
    pub fn test_serde_person_event_delete() {
        let person_ref = PersonEvent {
            person_id: 10,
            data: Patch::Null
        };

        let json_ref = r#"{"personId":10,"data":null}"#;

        serde_and_verify(&person_ref, json_ref);
    }

    fn serde_and_verify<'a, PersonEvent>(person_ref: &PersonEvent, json_ref: &'a str)
        where PersonEvent: Serialize + Deserialize<'a> + PartialEq + Debug {

        // 1. Serialize person_ref and string-compare it to json_ref
        let json = serde_json::to_string(&person_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with person_ref
        let person: Result<PersonEvent, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person.is_ok());
        assert_eq!(person.unwrap(), *person_ref);
    }
}
