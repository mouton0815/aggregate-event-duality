use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_patch::PersonPatch;

pub struct PersonEventBuilder;

impl PersonEventBuilder {
    pub fn for_insert(person_id: u32, person: &PersonData) -> Option<String> {
        Self::stringify(PersonEvent::for_insert(person_id, person))
    }

    pub fn for_update(person_id: u32, before: &PersonData, after: &PersonData) -> Option<String> {
        let patch = PersonPatch::of(before, after);
        if patch.is_noop() {
            None
        } else {
            Self::stringify(PersonEvent::for_update(person_id, &patch))
        }
    }

    pub fn for_delete(person_id: u32) -> Option<String> {
        Self::stringify(PersonEvent::for_delete(person_id))
    }

    fn stringify(event: PersonEvent) -> Option<String> {
        Some(serde_json::to_string(&event).unwrap()) // Errors should not happen, panic accepted
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_event_builder::PersonEventBuilder;

    #[test]
    pub fn test_for_insert() {
        let person = PersonData::new("Hans", None, Some(123));
        let event = r#"{"5":{"name":"Hans","spouseId":123}}"#;
        assert_eq!(PersonEventBuilder::for_insert(5, &person).unwrap(), event);
    }

    #[test]
    pub fn test_for_update() {
        let before = PersonData::new("Inge", Some("Here"), Some(123));
        let after = PersonData::new("Hans", None, Some(123));
        let event = r#"{"5":{"name":"Hans","location":null}}"#;
        assert_eq!(PersonEventBuilder::for_update(5, &before, &after).unwrap(), event);
    }

    #[test]
    pub fn test_for_update_noop() {
        let before = PersonData::new("Hans", None, Some(123));
        let after = PersonData::new("Hans", None, Some(123));
        assert_eq!(PersonEventBuilder::for_update(5, &before, &after), None);
    }

    #[test]
    pub fn test_for_delete() {
        let event = r#"{"5":null}"#;
        assert_eq!(PersonEventBuilder::for_delete(5).unwrap(), event);
    }
}