use std::collections::HashMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

///
/// A person event. The encapsulated map always contains exactly one person.
/// The implementation was chosen to produce the desired json output
/// <code>{ <person_id>: <person_data> }</code>.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonEvent(HashMap<u32, Option<PersonPatch>>);

impl PersonEvent {
    fn new(person_id: u32, person_data: Option<PersonPatch>) -> Self {
        let mut map = HashMap::new();
        map.insert(person_id, person_data);
        Self{ 0: map }
    }

    pub fn for_insert(person_id: u32, person: &PersonData) -> Self {
        Self::new(person_id, Some(PersonPatch {
            name: Some(person.name.clone()),
            location: Patch::of_option(&person.location, false),
            spouse_id: Patch::of_option(&person.spouse_id, false)
        }))
    }

    pub fn for_update(person_id: u32, person: &PersonPatch) -> Self {
        Self::new(person_id, Some(person.clone()))
    }

    pub fn for_delete(person_id: u32) -> Self {
        Self::new(person_id, None)
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_event::PersonEvent;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_person_event_values() {
        let person_event = PersonEvent::for_insert(1, &PersonData{
            name: "Hans".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: Some(2)
        });

        let json_ref = r#"{"1":{"name":"Hans","location":"Berlin","spouseId":2}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    pub fn test_person_event_absent() {
        let person_event = PersonEvent::for_update(1, &PersonPatch{
            name: None,
            location: Patch::Absent,
            spouse_id: Patch::Absent
        });

        let json_ref = r#"{"1":{}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_values() {
        let person_event = PersonEvent::for_update(1, &PersonPatch{
            name: None,
            location: Patch::Null,
            spouse_id: Patch::Null
        });

        let json_ref = r#"{"1":{"location":null,"spouseId":null}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    pub fn test_person_event_null_object() {
        let person_event = PersonEvent::for_delete(1);
        let json_ref = r#"{"1":null}"#;
        serde_and_verify(&person_event, json_ref);
    }
}