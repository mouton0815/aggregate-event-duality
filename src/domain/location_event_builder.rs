use crate::domain::location_event::LocationEvent;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;

pub struct LocationEventBuilder;

impl LocationEventBuilder {
    pub fn for_insert(person_id: u32, person: &PersonData) -> Option<String> {
        if person.location.is_some() {
            let location = person.location.as_ref().unwrap();
            Self::stringify(LocationEvent::for_insert_person(location, person_id, person))
        } else {
            None
        }
    }

    pub fn for_update(person_id: u32, before: &PersonData, after: &PersonData, is_last_in_aggregate: bool) -> Option<String> {
        let old_location = before.location.as_ref();
        let new_location = after.location.as_ref();
        if old_location.is_none() && new_location.is_none() {
            // No location information before and after
            None
        } else if old_location.is_none() && new_location.is_some() {
            // Update sets a location
            Self::stringify(LocationEvent::for_insert_person(new_location.unwrap(), person_id, after))
        } else if new_location.is_none() {
            // Update clears the location
            Self::stringify(LocationEvent::for_delete_person(old_location.unwrap(), person_id, is_last_in_aggregate))
        } else if new_location.is_none() || new_location.is_some() && old_location.unwrap() == new_location.unwrap() {
            // Update keeps the location
            let patch = PersonPatch::of(&before, &after);
            Self::stringify(LocationEvent::for_update_person(old_location.unwrap(), person_id, &patch))
        } else {
            // Update changes the location
            Self::stringify(LocationEvent::for_move_person(old_location.unwrap(), new_location.unwrap(), person_id, after, is_last_in_aggregate))
        }
    }

    pub fn for_delete(person_id: u32, person: &PersonData, is_last_in_aggregate: bool) -> Option<String> {
        let location = person.location.as_ref();
        if location.is_none() {
            None
        } else {
            Self::stringify(LocationEvent::for_delete_person(location.unwrap(), person_id, is_last_in_aggregate))
        }
    }

    fn stringify(event: LocationEvent) -> Option<String> {
        Some(serde_json::to_string(&event).unwrap()) // Errors should not happen, panic accepted
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_event_builder::LocationEventBuilder;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_insert_event_no_location() {
        let person = PersonData::new("", None, None);
        let result = LocationEventBuilder::for_insert(5, &person);
        assert_eq!(result, None); // No event created
    }

    #[test]
    pub fn test_insert_event_with_location() {
        let person = PersonData::new("Hans", Some("foo"), None);
        let result = LocationEventBuilder::for_insert(5, &person);
        assert_eq!(result, Some(r#"{"foo":{"5":{"name":"Hans","location":"foo"}}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_no_location() {
        let person = PersonData::new("", None, None);
        let result = LocationEventBuilder::for_update(5, &person, &person, false);
        assert_eq!(result, None); // No event created
    }

    #[test]
    pub fn test_update_event_set_location() {
        let before = PersonData::new("", None, None);
        let after = PersonData::new("Hans", Some("foo"), None);
        let result = LocationEventBuilder::for_update(5, &before, &after, false);
        assert_eq!(result, Some(r#"{"foo":{"5":{"name":"Hans","location":"foo"}}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_same_location() {
        let before = PersonData::new("Hans", Some("foo"), Some(123));
        let after = PersonData::new("Hans", Some("foo"), None);
        let result = LocationEventBuilder::for_update(5, &before, &after, false);
        assert_eq!(result, Some(r#"{"foo":{"5":{"spouseId":null}}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_change_location() {
        let before = PersonData::new("Hans", Some("foo"), Some(123));
        let after = PersonData::new("Inge", Some("bar"), None);
        let result = LocationEventBuilder::for_update(5, &before, &after, false);
        assert_eq!(result, Some(r#"{"bar":{"5":{"name":"Inge","location":"bar"}},"foo":{"5":null}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_change_last_location() {
        let before = PersonData::new("Hans", Some("foo"), Some(123));
        let after = PersonData::new("Hans", Some("bar"), None);
        let result = LocationEventBuilder::for_update(5, &before, &after, true);
        assert_eq!(result, Some(r#"{"bar":{"5":{"name":"Hans","location":"bar"}},"foo":null}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_remove_location() {
        let before = PersonData::new("Hans", Some("foo"), None);
        let after = PersonData::new("Inge", None, None);
        let result = LocationEventBuilder::for_update(5, &before, &after, false);
        assert_eq!(result, Some(r#"{"foo":{"5":null}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_remove_last_location() {
        let before = PersonData::new("Hans", Some("foo"), None);
        let after = PersonData::new("Inge", None, None);
        let result = LocationEventBuilder::for_update(5, &before, &after, true);
        assert_eq!(result, Some(r#"{"foo":null}"#.to_string()));
    }

    #[test]
    pub fn test_delete_event_remove_location() {
        let person = PersonData::new("", Some("foo"), None);
        let result = LocationEventBuilder::for_delete(5, &person, false);
        assert_eq!(result, Some(r#"{"foo":{"5":null}}"#.to_string()));
    }

    #[test]
    pub fn test_delete_event_remove_last_location() {
        let person = PersonData::new("", Some("foo"), None);
        let result = LocationEventBuilder::for_delete(5, &person, true);
        assert_eq!(result, Some(r#"{"foo":null}"#.to_string()));
    }
}