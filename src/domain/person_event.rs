use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_patch::PersonPatch;

/// A person event. The encapsulated map always contains exactly one person.
/// The implementation was chosen to produce the desired json output
/// <code>{ <person_id>: <person_data> }</code>.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonEvent(BTreeMap<u32, Option<PersonPatch>>);

impl PersonEvent {
    pub fn of(person_id: u32, person_data: Option<PersonPatch>) -> Self {
        let mut map = BTreeMap::new();
        map.insert(person_id, person_data);
        Self{ 0: map }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use serde::{Deserialize, Serialize};
    use crate::domain::person_event::PersonEvent;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_person_event_values() {
        let person_event = PersonEvent::of(1, Some(PersonPatch{
            name: Some("Hans".to_string()),
            location: Patch::Value("Berlin".to_string()),
            spouse_id: Patch::Value(2)
        }));

        let json_ref = r#"{"1":{"name":"Hans","location":"Berlin","spouseId":2}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    pub fn test_person_event_absent() {
        let person_event = PersonEvent::of(1, Some(PersonPatch{
            name: None,
            location: Patch::Absent,
            spouse_id: Patch::Absent
        }));

        let json_ref = r#"{"1":{}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_values() {
        let person_event = PersonEvent::of(1, Some(PersonPatch{
            name: None,
            location: Patch::Null,
            spouse_id: Patch::Null
        }));

        let json_ref = r#"{"1":{"location":null,"spouseId":null}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_object() {
        let person_event = PersonEvent::of(1, None);
        let json_ref = r#"{"1":null}"#;
        serde_and_verify(&person_event, json_ref);
    }

    fn serde_and_verify(person_event_ref: &PersonEvent, json_ref: & str) {
        // 1. Serialize person_map_ref and string-compare it to json_ref
        let json = serde_json::to_string(&person_event_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with person_map_ref
        let person_event: Result<PersonEvent, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person_event.is_ok());
        assert_eq!(person_event.unwrap(), *person_event_ref);
    }
}