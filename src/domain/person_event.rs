use std::collections::HashMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_data::PersonData;
use crate::domain::person_id::PersonId;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

///
/// A person event. The encapsulated map always contains exactly one
/// [PersonPatch](crate::domain::person_patch::PersonPatch) object.
/// The implementation was chosen to produce the desired json output
/// <code>{ <person_id>: <person_data> }</code>.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonEvent(HashMap<PersonId, Option<PersonPatch>>);

impl PersonEvent {
    fn new(person_id: PersonId, person_data: Option<PersonPatch>) -> Self {
        let mut map = HashMap::new();
        map.insert(person_id, person_data);
        Self{ 0: map }
    }

    pub fn for_insert(person_id: PersonId, person: &PersonData) -> Self {
        Self::new(person_id, Some(PersonPatch {
            name: Some(person.name.clone()),
            city: Patch::of_option(&person.city, false),
            spouse: Patch::of_option(&person.spouse, false)
        }))
    }

    pub fn for_update(person_id: PersonId, person: &PersonPatch) -> Self {
        Self::new(person_id, Some(person.clone()))
    }

    pub fn for_delete(person_id: PersonId) -> Self {
        Self::new(person_id, None)
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_event::PersonEvent;
    use crate::domain::person_id::PersonId;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    fn test_person_event_values() {
        let person = PersonData::new("Hans", Some("Berlin"), Some(PersonId::from(2)));
        let person_event = PersonEvent::for_insert(PersonId::from(1), &person);

        let json_ref = r#"{"1":{"name":"Hans","city":"Berlin","spouse":2}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    fn test_person_event_absent() {
        let patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let person_event = PersonEvent::for_update(PersonId::from(1), &patch);

        let json_ref = r#"{"1":{}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    fn test_person_event_null_values() {
        let patch = PersonPatch::new(None, Patch::Null, Patch::Null);
        let person_event = PersonEvent::for_update(PersonId::from(1), &patch);

        let json_ref = r#"{"1":{"city":null,"spouse":null}}"#;
        serde_and_verify(&person_event, json_ref);
    }

    #[test]
    fn test_person_event_null_object() {
        let person_event = PersonEvent::for_delete(PersonId::from(1));
        let json_ref = r#"{"1":null}"#;
        serde_and_verify(&person_event, json_ref);
    }
}