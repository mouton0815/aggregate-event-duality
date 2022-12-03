use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_patch::PersonPatch;

/// A location event. The encapsulated map always contains one or two person events.
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

    pub fn for_insert_person(location: &str, person_id: u32, person: &PersonData) -> Self {
        Self::new(location, Some(PersonEvent::for_insert(person_id, person)))
    }

    pub fn for_update_person(location: &str, person_id: u32, person: &PersonPatch) -> Self {
        Self::new(location, Some(PersonEvent::for_update(person_id, person)))
    }

    pub fn for_move_person(old_location: &str, new_location: &str, person_id: u32, person: &PersonData, is_last_in_old_location: bool) -> Self {
        let mut event = Self::new(new_location, Some(PersonEvent::for_insert(person_id, person)));
        event.0.insert(old_location.to_string(), match is_last_in_old_location {
            false => Some(PersonEvent::for_delete(person_id)),
            true => None
        });
        event
    }

    pub fn for_delete_person(location: &str, person_id: u32, is_last_in_location: bool) -> Self {
        Self::new(location, match is_last_in_location {
            false => Some(PersonEvent::for_delete(person_id)),
            true => None
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_event::LocationEvent;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_for_insert_person() {
        let person = PersonData::new("Hans", Some("Here"), None);
        let event = LocationEvent::for_insert_person("Here", 3, &person);
        let json_ref = r#"{"Here":{"3":{"name":"Hans","location":"Here"}}}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    pub fn test_for_update_person() {
        let person = PersonPatch::new(Some("Hans"), Patch::Null, Patch::Absent);
        let event = LocationEvent::for_update_person("Here", 5, &person);
        let json_ref = r#"{"Here":{"5":{"name":"Hans","location":null}}}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    pub fn test_for_move_person() {
        let person = PersonData::new("Hans", Some("Here"), None);
        let event = LocationEvent::for_move_person("Here", "There", 5, &person, false);
        let json_ref = r#"{"Here":{"5":null},"There":{"5":{"name":"Hans","location":"Here"}}}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    pub fn test_for_move_person_and_delete_location() {
        let person = PersonData::new("Hans", Some("Here"), None);
        let event = LocationEvent::for_move_person("Here", "There", 5, &person, true);
        let json_ref = r#"{"Here":null,"There":{"5":{"name":"Hans","location":"Here"}}}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    pub fn test_for_delete_person() {
        let event = LocationEvent::for_delete_person("Here", 7, false);
        let json_ref = r#"{"Here":{"7":null}}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    pub fn test_for_delete_location() {
        let event = LocationEvent::for_delete_person("Here", 7, true);
        let json_ref = r#"{"Here":null}"#;
        serde_and_verify(&event, json_ref);
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