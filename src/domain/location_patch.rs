use log::warn;
use serde::{Serialize, Deserialize};
use crate::domain::location_data::LocationData;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

///
/// The body of a [LocationEvent](crate::domain::location_event::LocationEvent).
/// A ``LocationEvent`` represents changes of statistical information (i.e. counters) about
/// persons with respect to a location. A serialized ``LocationEvent`` contains only counters
/// that changed, all others are left out. This is modeled with [Option](core::option) wrappers.
///
/// ``LocationPatch`` objects are constructed from a
/// [LocationData](crate::domain::location_data::LocationData) record and from data
/// of the person that caused the update.
///
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationPatch {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>, // Attribute can be updated or left as is, but not deleted

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub married: Option<usize>
}

impl LocationPatch {
    /// Private constructor
    fn new(total: Option<usize>, married: Option<usize>) -> Self {
        Self { total, married }
    }

    ///
    /// Constructs a ``LocationPatch`` object with changes on a ``LocationData`` object
    /// after the insertion of a person.
    /// * data - the location aggregate
    /// * person - the inserted person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_insert(data: &LocationData, person: &PersonData) -> Option<Self> {
        if person.city.is_some() { // Should be checked by the caller (could be an assertion)
            let first = data.total == 0;
            let total = Some(data.total + 1);
            let married = Self::conditional_increment(data.married, person.spouse, first);
            // Further updates of data fields here ...
            Some(Self::new(total, married))
        } else {
            None
        }
    }

    ///
    /// Constructs a ``LocationPatch`` object with changes on a ``LocationData`` object
    /// in case the location of the person is unchanged.
    /// * data - the location aggregate
    /// * person - the person record _before_ the update
    /// * patch - the change set to be applied to the person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_update(data: &LocationData, person: &PersonData, patch: &PersonPatch) -> Option<Self> {
        // Should be checked by the caller (could be an assertion):
        if person.city.is_some() && patch.city.is_absent() {
            // Location of person remains, adapt all counters except total
            let married = match patch.spouse {
                Patch::Value(_) => Some(data.married + 1),
                Patch::Null => Self::checked_decrement(data.married),
                Patch::Absent => None
            };
            // Further updates of data fields here ...
            if married.is_some() {
                return Some(Self::new(None, married));
            }
        }
        None
    }

    ///
    /// Constructs a ``LocationPatch`` object with changes on the ``LocationData`` object
    /// that represents the new location of a person after a location change.
    /// * data - the location aggregate
    /// * person - the person record _before_ the update
    /// * patch - the change set to be applied to the person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_change(data: &LocationData, person: &PersonData, patch: &PersonPatch) -> Option<Self> {
        // Location of person changed, decrement counters of new location
        if patch.city.is_value() { // Should be checked by the caller (could be an assertion)
            let first = data.total == 0;
            let total = Some(data.total + 1);
            let married = match patch.spouse {
                Patch::Value(_) => Some(data.married + 1),
                Patch::Null => if first { Some(0) } else { None },
                Patch::Absent => Self::conditional_increment(data.married, person.spouse, first)
            };
            // Further updates of data fields here ...
            Some(Self::new(total, married))
        } else {
            None
        }
    }

    ///
    /// Constructs a ``LocationPatch`` object with changes on a ``LocationData`` object
    /// after the deletion of a person. This method is also used for updating the old
    /// ``LocationData`` object if a person changed the location..
    /// * data - the location aggregate
    /// * person - the deleted person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_delete(data: &LocationData, person: &PersonData) -> Option<Self> {
        if person.city.is_some() { // Should be checked by the caller (could be an assertion)
            let total = Self::checked_decrement(data.total);
            let married = match person.spouse {
                Some(_) => Self::checked_decrement(data.married),
                None => None
            };
            // Further updates of data fields here ...
            if total.is_some() || married.is_some() {
                return Some(Self::new(total, married));
            }
        }
        None
    }

    //
    // Private helpers
    //

    /// Increments ``value`` only if attribute ``control`` is ``Some``.
    /// Otherwise, ``first`` decides if is a new entry, which must be initialized with ``Some(0)``.
    fn conditional_increment<T>(value: usize, control: Option<T>, first: bool) -> Option<usize> {
        match control {
            Some(_) => Some(value + 1),
            None => if first { Some(0) } else { None }
        }
    }

    fn checked_decrement(value: usize) -> Option<usize> {
        if value == 0 {
            warn!("Counter is already 0, do not decrement");
            None
        } else {
            Some(value - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_data::LocationData;
    use crate::domain::location_patch::LocationPatch;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_id::PersonId;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    //
    // Tests for serializing/deserializing
    //

    #[test]
    fn test_serde_some() {
        let data_ref = LocationPatch::new(Some(1), Some(3));
        let json_ref = r#"{"total":1,"married":3}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    #[test]
    fn test_serde_none() {
        let data_ref = LocationPatch::new(None, None);
        let json_ref = r#"{}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    //
    // Tests for method for_insert
    //

    // Convenience method for constructing inserts into an existing location aggregate (1, 3)
    fn for_insert(person: PersonData) -> Option<LocationPatch> {
        LocationPatch::for_insert(&LocationData::new(1, 3), &person)
    }

    // Convenience method for constructing insert into a new location aggregate (0, 0)
    fn for_insert_initial(person: PersonData) -> Option<LocationPatch> {
        LocationPatch::for_insert(&LocationData::new(0, 0), &person)
    }

    #[test]
    fn test_for_insert() {
        let patch = for_insert(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_insert_no_spouse() {
        let patch = for_insert(
            PersonData::new("Ann", Some("here"), None));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_insert_initial_no_spouse() {
        let patch = for_insert_initial(
            PersonData::new("Ann", Some("here"), None));
        assert_eq!(patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    #[test]
    fn test_for_insert_no_location() {
        let patch = for_insert(
            PersonData::new("Ann", None, None));
        assert_eq!(patch, None); // No location, no result
    }

    //
    // Tests for method for_update
    //

    // Convenience method for constructing updates of an existing location aggregate (1, 3)
    fn for_update(person: PersonData, patch: PersonPatch) -> Option<LocationPatch> {
        LocationPatch::for_update(&LocationData::new(1, 3), &person, &patch)
    }

    // Convenience method for constructing updates of a new location aggregate (0, 0)
    fn for_update_initial(person: PersonData, patch: PersonPatch) -> Option<LocationPatch> {
        LocationPatch::for_update(&LocationData::new(0, 0), &person, &patch)
    }

    #[test]
    fn test_for_update_no_location() {
        let patch = for_update(
            PersonData::new("Ann", None, Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Absent, Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_update_keep_location_keep_spouse() {
        let patch = for_update(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Absent, Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_update_keep_location_remove_spouse() {
        let patch = for_update(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Absent, Patch::Null));
        assert_eq!(patch, Some(LocationPatch::new(None, Some(2))));
    }

    #[test]
    fn test_for_update_keep_location_remove_spouse_below_zero() {
        let patch = for_update_initial( // Not realistic as a location exists, but anyway
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Absent, Patch::Null));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_update_keep_location_set_spouse() {
        let patch = for_update(
            PersonData::new("Ann", Some("here"), None),
            PersonPatch::new(None, Patch::Absent, Patch::Value(PersonId::from(123))));
        assert_eq!(patch, Some(LocationPatch::new(None, Some(4))));
    }

    #[test]
    fn test_for_update_remove_location() {
        let patch = for_update(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Null, Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_update_set_location() {
        let patch = for_update(
            PersonData::new("Ann", None, Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_update_change_location() {
        let patch = for_update(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("there"), Patch::Absent));
        assert_eq!(patch, None);
    }

    //
    // Tests for method for_change
    //

    // Convenience method for constructing changes of an existing location aggregate (1, 3)
    fn for_change(person: PersonData, patch: PersonPatch) -> Option<LocationPatch> {
        LocationPatch::for_change(&LocationData::new(1, 3), &person, &patch)
    }

    // Convenience method for constructing changes of a new location aggregate (0, 0)
    fn for_change_initial(person: PersonData, patch: PersonPatch) -> Option<LocationPatch> {
        LocationPatch::for_change(&LocationData::new(0, 0), &person, &patch)
    }

    #[test]
    fn test_for_change_no_location() {
        let patch = for_change(
            PersonData::new("Ann", None, Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Absent, Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_change_keep_location() {
        let patch = for_change(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Absent, Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_change_remove_location() {
        let patch = for_change(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Null, Patch::Absent));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_change_set_location() {
        let patch = for_change(
            PersonData::new("Ann", None, Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_change_set_location_no_spouse() {
        let patch = for_change(
            PersonData::new("Ann", None, None),
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_set_location_initial_no_spouse() {
        let patch = for_change_initial(
            PersonData::new("Ann", None, None),
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent));
        assert_eq!(patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    #[test]
    fn test_for_change_set_location_remove_spouse() {
        let patch = for_change(
            PersonData::new("Ann", None, Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("here"), Patch::Null));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_set_location_initial_remove_spouse() {
        let patch = for_change_initial(
            PersonData::new("Ann", None, Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("here"), Patch::Null));
        assert_eq!(patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    #[test]
    fn test_for_change_alter_location_no_spouse() {
        let patch = for_change(
            PersonData::new("Ann", Some("here"), None),
            PersonPatch::new(None, Patch::Value("there"), Patch::Absent));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_alter_location_keep_spouse() {
        let patch = for_change(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("there"), Patch::Absent));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_change_alter_location_set_spouse() {
        let patch = for_change(
            PersonData::new("Ann", Some("here"), None),
            PersonPatch::new(None, Patch::Value("there"), Patch::Value(PersonId::from(123))));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_change_alter_location_remove_spouse() {
        let patch = for_change(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("there"), Patch::Null));
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_alter_location_initial_remove_spouse() {
        let patch = for_change_initial(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))),
            PersonPatch::new(None, Patch::Value("there"), Patch::Null));
        assert_eq!(patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    //
    // Tests for method for_delete
    //

    // Convenience method for constructing deletions of an existing location aggregate (1, 3)
    fn for_delete(person: PersonData) -> Option<LocationPatch> {
        LocationPatch::for_delete(&LocationData::new(1, 3), &person)
    }

    // Convenience method for constructing (impossible) deletions of a new location aggregate (0, 0)
    fn for_delete_initial(person: PersonData) -> Option<LocationPatch> {
        LocationPatch::for_delete(&LocationData::new(0, 0), &person)
    }

    #[test]
    fn test_for_delete() {
        let patch = for_delete(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))));
        assert_eq!(patch, Some(LocationPatch::new(Some(0), Some(2))));
    }

    #[test]
    fn test_for_delete_no_spouse() {
        let patch = for_delete(
            PersonData::new("Ann", Some("here"), None));
        assert_eq!(patch, Some(LocationPatch::new(Some(0), None)));
    }

    #[test]
    fn test_for_delete_no_location() {
        let patch = for_delete(
            PersonData::new("Ann", None, None));
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_delete_below_zero() {
        let patch = for_delete_initial(
            PersonData::new("Ann", Some("here"), Some(PersonId::from(123))));
        assert_eq!(patch, None);
    }
}
