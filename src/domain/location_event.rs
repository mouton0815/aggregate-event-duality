use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_event::PersonEvent;

/// A location event. The encapsulated map always contains exactly one person event.
/// The implementation was chosen to produce the desired json output
/// <code>{ <location>: { <person_id>: <person_data> }}</code>.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationEvent(BTreeMap<String, Option<PersonEvent>>);

impl LocationEvent {
    fn new(location: &str, person_event: Option<PersonEvent>) -> Self {
        let mut map = BTreeMap::new();
        map.insert(location.to_string(), person_event);
        Self{ 0: map }
    }

    pub fn for_upsert(location: &str, person_event: PersonEvent) -> Self {
        Self::new(location, Some(person_event))
    }

    pub fn for_delete(location: &str) -> Self {
        Self::new(location, None)
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_event::LocationEvent;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_event::PersonEvent;

    #[test]
    pub fn test_location_event_values() {
        let person_data = PersonData::new("Hans", Some("Here"), None);
        let person_event = PersonEvent::for_insert(3, &person_data);
        let location_event = LocationEvent::for_upsert("Here", person_event);
        let json_ref = r#"{"Here":{"3":{"name":"Hans","location":"Here"}}}"#;
        serde_and_verify(&location_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_person() {
        let person_event = PersonEvent::for_delete(7);
        let location_event = LocationEvent::for_upsert("Here", person_event);
        let json_ref = r#"{"Here":{"7":null}}"#;
        serde_and_verify(&location_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_location() {
        let location_event = LocationEvent::for_delete("Here");
        let json_ref = r#"{"Here":null}"#;
        serde_and_verify(&location_event, json_ref);
    }

    fn serde_and_verify(location_event_ref: &LocationEvent, json_ref: &str) {
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