use log::warn;
use serde::{Serialize, Deserialize};
use crate::domain::location_data::LocationData;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

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
    pub fn new(total: Option<usize>, married: Option<usize>) -> Self {
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
        if person.location.is_some() { // Should be checked by the caller (could be an assertion)
            let total = Some(data.total + 1);
            let married = person.spouse_id.map(|_| data.married + 1);
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
        let mut married : Option<usize> = None;
        // Should be checked by the caller (could be an assertion):
        if person.location.is_some() && patch.location.is_absent() {
            // Location of person remains, adapt all counters except total
            married = match patch.spouse_id {
                Patch::Value(_) => Some(data.married + 1),
                Patch::Null => Self::checked_decrement(data.married),
                Patch::Absent => None
            };
        }
        if married.is_some() {
            Some(Self::new(None, married))
        } else {
            None
        }
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
        if patch.location.is_value() { // Should be checked by the caller (could be an assertion)
            let total = Some(data.total + 1);
            let married = match patch.spouse_id {
                Patch::Value(_) => Some(data.married + 1),
                Patch::Null => None,
                Patch::Absent => person.spouse_id.map(|_| data.married + 1)
            };
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
        let mut total : Option<usize> = None;
        let mut married : Option<usize> = None;
        if person.location.is_some() { // Should be checked by the caller (could be an assertion)
            total = Self::checked_decrement(data.total);
            married = match person.spouse_id {
                Some(_) => Self::checked_decrement(data.married),
                None => None
            }
        }
        if total.is_some() || married.is_some() {
            Some(Self::new(total, married))
        } else {
            None
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
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    //
    // Tests for serializing/deserializing
    //

    #[test]
    pub fn test_serde_some() {
        let data_ref = LocationPatch::new(Some(1), Some(3));
        let json_ref = r#"{"total":1,"married":3}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    #[test]
    pub fn test_serde_none() {
        let data_ref = LocationPatch::new(None, None);
        let json_ref = r#"{}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    //
    // Tests for method for_insert
    //

    #[test]
    pub fn test_for_insert() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let patch = LocationPatch::for_insert(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    pub fn test_for_insert_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::for_insert(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    pub fn test_for_insert_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let patch = LocationPatch::for_insert(&aggr, &person);
        assert_eq!(patch, None);
    }

    //
    // Tests for method for_update
    //

    #[test]
    pub fn test_for_update_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_keep_location_keep_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_keep_location_remove_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Null);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(None, Some(2))));
    }

    #[test]
    pub fn test_for_update_keep_location_remove_spouse_below_zeri() {
        let aggr = LocationData::new(1, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Null);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_keep_location_set_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Value(123));
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(None, Some(4))));
    }

    #[test]
    pub fn test_for_update_remove_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_set_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_change_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_update(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    //
    // Tests for method for_change
    //

    #[test]
    pub fn test_for_change_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_change_keep_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_change_remove_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_change_set_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    pub fn test_for_change_set_location_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, None);
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    pub fn test_for_change_set_location_remove_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(124));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Null);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    pub fn test_for_change_change_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    pub fn test_for_change_change_location_set_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Value(123));
        let l_patch = LocationPatch::for_change(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    //
    // Tests for method for_delete
    //

    #[test]
    pub fn test_for_delete() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let patch = LocationPatch::for_delete(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(0), Some(2))));
    }

    #[test]
    pub fn test_for_delete_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::for_delete(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(Some(0), None)));
    }

    #[test]
    pub fn test_for_delete_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let patch = LocationPatch::for_delete(&aggr, &person);
        assert_eq!(patch, None);
    }

    #[test]
    pub fn test_for_delete_below_zero() {
        let aggr = LocationData::new(0, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let patch = LocationPatch::for_delete(&aggr, &person);
        assert_eq!(patch, None);
    }
}
