use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_patch::PersonPatch;

pub struct PersonEventBuilder;

impl PersonEventBuilder {
    pub fn for_insert(person_id: u32, person: &PersonData) -> String {
        Self::stringify(PersonEvent::for_insert(person_id, person))
    }

    pub fn for_update(person_id: u32, person: &PersonPatch) -> String {
        Self::stringify(PersonEvent::for_update(person_id, person))
    }

    pub fn for_delete(person_id: u32) -> String {
        Self::stringify(PersonEvent::for_delete(person_id))
    }

    fn stringify(event: PersonEvent) -> String {
        serde_json::to_string(&event).unwrap() // Errors should not happen, panic accepted
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_event_builder::PersonEventBuilder;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_for_insert() {
        let person = PersonData::new("Hans", None, Some(123));
        let event = r#"{"5":{"name":"Hans","spouseId":123}}"#;
        assert_eq!(PersonEventBuilder::for_insert(5, &person), event);
    }

    #[test]
    pub fn test_for_update() {
        let person = PersonPatch::new(Some("Hans"), Patch::Null, Patch::Absent);
        let event = r#"{"5":{"name":"Hans","location":null}}"#;
        assert_eq!(PersonEventBuilder::for_update(5, &person), event);
    }

    #[test]
    pub fn test_for_delete() {
        let event = r#"{"5":null}"#;
        assert_eq!(PersonEventBuilder::for_delete(5), event);
    }
}