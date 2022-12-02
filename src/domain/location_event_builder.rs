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

    pub fn for_update(person_id: u32, person_patch: &PersonPatch, person_data: &PersonData, old_location: Option<&str>, is_last_with_old_location: bool) -> Option<String> {
        let new_location = person_patch.location.as_ref();
        if old_location.is_none() && !new_location.is_value() {
            // No location information before and after
            None
        } else if old_location.is_none() && new_location.is_value() {
            // Update sets a location
            Self::stringify(LocationEvent::for_insert_person(new_location.unwrap(), person_id, person_data))
        } else if new_location.is_null() && is_last_with_old_location {
            // Update clears the location (the person was the last one with old_location)
            Self::stringify(LocationEvent::for_delete_location(old_location.unwrap()))
        } else if new_location.is_null() {
            // Update clears the location (there are other persons with old_location)
            Self::stringify(LocationEvent::for_delete_person(old_location.unwrap(), person_id))
        } else if new_location.is_absent() || new_location.is_value() && old_location.unwrap() == new_location.unwrap() {
            // Update keeps the location
            Self::stringify(LocationEvent::for_update_person(old_location.unwrap(), person_id, person_patch))
        } else if is_last_with_old_location {
            // Update changes the location (the person was the last one with old_location)
            Self::stringify(LocationEvent::for_move_person_and_delete_location(old_location.unwrap(), new_location.unwrap(), person_id, person_data))
        } else {
            // Update changes the location (there are other persons with old_location)
            Self::stringify(LocationEvent::for_move_person(old_location.unwrap(), new_location.unwrap(), person_id, person_data))
        }
    }

    pub fn for_delete(person_id: u32, old_location: Option<&str>, is_last_with_old_location: bool) -> Option<String> {
        if old_location.is_none() {
            None
        } else if is_last_with_old_location {
            Self::stringify(LocationEvent::for_delete_location(old_location.unwrap()))
        } else {
            Self::stringify(LocationEvent::for_delete_person(old_location.unwrap(), person_id))
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
        let person = PersonData::new("Hans", None, None);
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
        let patch = PersonPatch::new(None, Patch::Null, Patch::Null);
        let after = PersonData::new("Hans", None, None);
        let result = LocationEventBuilder::for_update(5, &patch, &after, None, false);
        assert_eq!(result, None); // No event created
    }

    #[test]
    pub fn test_update_event_set_location() {
        let patch = PersonPatch::new(None, Patch::Value("foo"), Patch::Absent);
        let after = PersonData::new("Hans", Some("foo"), Some(123));
        let result = LocationEventBuilder::for_update(5, &patch, &after, None, false);
        assert_eq!(result, Some(r#"{"foo":{"5":{"name":"Hans","location":"foo","spouseId":123}}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_keep_location() {
        let patch = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Absent);
        let after = PersonData::new("Hans", Some("foo"), Some(123));
        let result = LocationEventBuilder::for_update(5, &patch, &after, Some("foo"), false);
        assert_eq!(result, Some(r#"{"foo":{"5":{"name":"Hans"}}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_same_location() {
        let patch = PersonPatch::new(None, Patch::Value("foo"), Patch::Absent);
        let after = PersonData::new("Hans", Some("foo"), Some(123));
        let result = LocationEventBuilder::for_update(5, &patch, &after, Some("foo"), false);
        assert_eq!(result, Some(r#"{"foo":{"5":{"location":"foo"}}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_change_location() {
        let patch = PersonPatch::new(None, Patch::Value("bar"), Patch::Absent);
        let after = PersonData::new("Hans", Some("bar"), None);
        let result = LocationEventBuilder::for_update(5, &patch, &after, Some("foo"), false);
        assert_eq!(result, Some(r#"{"bar":{"5":{"name":"Hans","location":"bar"}},"foo":{"5":null}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_change_last_location() {
        let patch = PersonPatch::new(None, Patch::Value("bar"), Patch::Absent);
        let after = PersonData::new("Hans", Some("bar"), None);
        let result = LocationEventBuilder::for_update(5, &patch, &after, Some("foo"), true);
        assert_eq!(result, Some(r#"{"bar":{"5":{"name":"Hans","location":"bar"}},"foo":null}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_remove_location() {
        let patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let after = PersonData::new("Hans", None, Some(123));
        let result = LocationEventBuilder::for_update(5, &patch, &after, Some("foo"), false);
        assert_eq!(result, Some(r#"{"foo":{"5":null}}"#.to_string()));
    }

    #[test]
    pub fn test_update_event_remove_last_location() {
        let patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let after = PersonData::new("Hans", None, Some(123));
        let result = LocationEventBuilder::for_update(5, &patch, &after, Some("foo"), true);
        assert_eq!(result, Some(r#"{"foo":null}"#.to_string()));
    }

    #[test]
    pub fn test_delete_event_remove_location() {
        let result = LocationEventBuilder::for_delete(5, Some("foo"), false);
        assert_eq!(result, Some(r#"{"foo":{"5":null}}"#.to_string()));
    }

    #[test]
    pub fn test_delete_event_remove_last_location() {
        let result = LocationEventBuilder::for_delete(5, Some("foo"), true);
        assert_eq!(result, Some(r#"{"foo":null}"#.to_string()));
    }
}