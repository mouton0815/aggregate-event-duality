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
            let always_include = data.total == 0;
            let total = Some(data.total + 1);
            let married = Self::value_for_insert(data.married, person.spouse, always_include);
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
            let married = Self::value_for_update(data.married, patch.spouse);
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
            let always_include = data.total == 0;
            let total = Some(data.total + 1);
            let married = Self::value_for_change(data.married, patch.spouse, person.spouse, always_include);
            // let married = Self::conditional_increment(data.married, person.spouse, patch.spouse, data.total);
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
            let married = Self::value_for_delete(data.married, person.spouse);
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

    // Calculates an aggregated value for the for_insert method depending on the
    // person attribute control. If the control attribute is Some, the aggregate value
    // is incremented. Otherwise, always_include controls the result: If it is true,
    // value is returned unchanged, else None is returned.
    fn value_for_insert<T>(value: usize, control: Option<T>, always_include: bool) -> Option<usize> {
        match control {
            Some(_) => Some(value + 1),
            None => if always_include { Some(value) } else { None }
        }
    }

    // Calculates an aggregated value for the for_update method depending on the
    // patch attribute control. If the control attribute has a Value, the aggregate
    // value is incremented. It it is Null, it is decremented (indicating the removal
    // of a person field). Lastly, if the control value is Absent, then the aggregate
    // value is omitted.
    fn value_for_update<T>(value: usize, patch: Patch<T>) -> Option<usize> {
        match patch {
            Patch::Value(_) => Some(value + 1),
            Patch::Null => Self::checked_decrement(value),
            Patch::Absent => None
        }
    }

    // Calculates an aggregated value for the for_change method depending on the
    // patch attribute control. If the control attribute has a Value, the aggregate
    // value is incremented. It it is Null, the result depends on always_include:
    // If it is true, value is returned unchanged, else None is returned.
    // Lastly, if the control value is Absent, then the aggregate value is incremented
    // only if id had a value before the change operation. This is controlled by the
    // control_insert parameter.
    fn value_for_change<T>(value: usize, control: Patch<T>, control_insert: Option<T>, always_include: bool) -> Option<usize> {
        match control {
            Patch::Value(_) => Some(value + 1),
            Patch::Null => if always_include { Some(value) } else { None },
            Patch::Absent => Self::value_for_insert(value, control_insert, always_include)
        }
    }

    // Calculates an aggregated value for the for_delete method depending on the
    // person attribute control. If the control attribute is Some, the aggregate
    // value is decremented. Otherwise, None is returned.
    fn value_for_delete<T>(value: usize, control: Option<T>) -> Option<usize> {
        match control {
            Some(_) => Self::checked_decrement(value),
            None => None
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

    #[test]
    fn test_for_insert() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let patch = LocationPatch::for_insert(&loc, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_insert_no_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::for_insert(&loc, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_insert_initial_no_spouse() {
        let loc = LocationData::new(0, 0);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::for_insert(&loc, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    #[test]
    fn test_for_insert_no_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let patch = LocationPatch::for_insert(&loc, &person);
        assert_eq!(patch, None);
    }

    //
    // Tests for method for_update
    //

    #[test]
    fn test_for_update_no_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_update_keep_location_keep_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_update_keep_location_remove_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Null);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(None, Some(2))));
    }

    #[test]
    fn test_for_update_keep_location_remove_spouse_below_zeri() {
        let loc = LocationData::new(1, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Null);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_update_keep_location_set_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Value(PersonId::from(123)));
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(None, Some(4))));
    }

    #[test]
    fn test_for_update_remove_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_update_set_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_update_change_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_update(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    //
    // Tests for method for_change
    //

    #[test]
    fn test_for_change_no_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_change_keep_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_change_remove_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    fn test_for_change_set_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_change_set_location_no_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, None);
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_set_location_initial_no_spouse() {
        let loc = LocationData::new(0, 0);
        let person = PersonData::new("Hans", None, None);
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    #[test]
    fn test_for_change_set_location_remove_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Null);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_set_location_initial_remove_spouse() {
        let loc = LocationData::new(0, 0);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Null);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    #[test]
    fn test_for_change_alter_location_no_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_alter_location_keep_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_change_alter_location_set_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Value(PersonId::from(123)));
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    fn test_for_change_alter_location_remove_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Null);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    fn test_for_change_alter_location_initial_remove_spouse() {
        let loc = LocationData::new(0, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Null);
        let l_patch = LocationPatch::for_change(&loc, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(1), Some(0)))); // Initial event, all values are set
    }

    //
    // Tests for method for_delete
    //

    #[test]
    fn test_for_delete() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let patch = LocationPatch::for_delete(&loc, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(0), Some(2))));
    }

    #[test]
    fn test_for_delete_no_spouse() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::for_delete(&loc, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(0), None)));
    }

    #[test]
    fn test_for_delete_no_location() {
        let loc = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(PersonId::from(123)));
        let patch = LocationPatch::for_delete(&loc, &person);
        assert_eq!(patch, None);
    }

    #[test]
    fn test_for_delete_below_zero() {
        let loc = LocationData::new(0, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(PersonId::from(123)));
        let patch = LocationPatch::for_delete(&loc, &person);
        assert_eq!(patch, None);
    }
}
