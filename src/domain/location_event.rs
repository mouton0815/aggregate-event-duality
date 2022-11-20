use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_event::PersonEvent;

/// A location event. The encapsulated map always contains exactly one person event.
/// The implementation was chosen to produce the desired json output
/// <code>{ <location>: { <person_id>: <person_data> }}</code>.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationEvent(BTreeMap<String, Option<PersonEvent>>);

impl LocationEvent {
    pub fn of(location: &str, person_event: Option<PersonEvent>) -> Self {
        let mut map = BTreeMap::new();
        map.insert(location.to_string(), person_event);
        Self{ 0: map }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_event::LocationEvent;
    use crate::domain::person_event::PersonEvent;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_location_event_values() {
        let person_event = PersonEvent::of(3, Some(PersonPatch{
            name: Some("Hans".to_string()),
            location: Patch::Value("Here".to_string()),
            spouse_id: Patch::Absent
        }));
        let location_event = LocationEvent::of("Here", Some(person_event));
        let json_ref = r#"{"Here":{"3":{"name":"Hans","location":"Here"}}}"#;
        serde_and_verify(&location_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_person() {
        let person_event = PersonEvent::of(7, None);
        let location_event = LocationEvent::of("Here", Some(person_event));
        let json_ref = r#"{"Here":{"7":null}}"#;
        serde_and_verify(&location_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_location() {
        let location_event = LocationEvent::of("Here", None);
        let json_ref = r#"{"Here":null}"#;
        serde_and_verify(&location_event, json_ref);
    }

    fn serde_and_verify(location_event_ref: &LocationEvent, json_ref: & str) {
        // 1. Serialize location_event_ref and string-compare it to json_ref
        let json = serde_json::to_string(&location_event_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with person_map_ref
        let location_event: Result<LocationEvent, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(location_event.is_ok());
        assert_eq!(location_event.unwrap(), *location_event_ref);
    }
}