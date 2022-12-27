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
    fn new(total: Option<usize>, married: Option<usize>) -> Self {
        Self { total, married }
    }

    ///
    /// Constructs a ``LocationPatch`` object reflecting changes related to the location of an inserted person
    /// * aggr - the location aggregate
    /// * person - the inserted person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_insert(aggr: &LocationData, person: &PersonData) -> Option<Self> {
        if person.location.is_some() { // Should be checked by the caller (could be an assertion)
            let married = person.spouse_id.map(|_| aggr.married + 1);
            Some(Self::new(Some(aggr.total + 1), married))
        } else {
            None
        }
    }

    ///
    /// Constructs a ``LocationPatch`` object reflecting changes related to the old location of a person
    /// * aggr - the location aggregate
    /// * person - the person record _before_ the update
    /// * patch - the change set to be applied to the person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_update_old(aggr: &LocationData, person: &PersonData, patch: &PersonPatch) -> Option<Self> {
        let mut total : Option<usize> = None;
        let mut married : Option<usize> = None;
        if person.location.is_some() { // Should be checked by the caller (could be an assertion)
            if patch.location.is_absent() {
                // Location of person remains, adapt all counters except total
                married = match patch.spouse_id {
                    Patch::Value(_) => Some(aggr.married + 1),
                    Patch::Null => Self::checked_decrement(aggr.married, "married"),
                    Patch::Absent => None
                };
            } else {
                // Location of person changed, decrement counters of old location
                total = Self::checked_decrement(aggr.total, "total");
                married = match person.spouse_id {
                    Some(_) => Self::checked_decrement(aggr.married, "married"),
                    None => None
                };
            }
        }
        if total.is_some() || married.is_some() {
            Some(Self::new(total, married))
        } else {
            None
        }
    }

    ///
    /// Constructs a ``LocationPatch`` object reflecting changes related to the new location of a person
    /// * aggr - the location aggregate
    /// * person - the person record _before_ the update
    /// * patch - the change set to be applied to the person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_update_new(aggr: &LocationData, person: &PersonData, patch: &PersonPatch) -> Option<Self> {
        // Location of person changed, decrement counters of new location
        if patch.location.is_value() { // Should be checked by the caller (could be an assertion)
            let total = Some(aggr.total + 1);
            let married = match patch.spouse_id {
                Patch::Value(_) => Some(aggr.married + 1),
                Patch::Null => None,
                Patch::Absent => person.spouse_id.map(|_| aggr.married + 1)
            };
            Some(Self::new(total, married))
        } else {
            None
        }
    }

    ///
    /// Constructs a ``LocationPatch`` object reflecting changes related to the location of a deleted person
    /// * aggr - the location aggregate
    /// * person - the deleted person record
    /// Returns a ``LocationPatch`` object or ``None`` if no aggregate value changed.
    ///
    pub fn for_delete(aggr: &LocationData, person: &PersonData) -> Option<Self> {
        let mut total : Option<usize> = None;
        let mut married : Option<usize> = None;
        if person.location.is_some() { // Should be checked by the caller (could be an assertion)
            total = Self::checked_decrement(aggr.total, "total");
            married = match person.spouse_id {
                Some(_) => Self::checked_decrement(aggr.married, "married"),
                None => None
            };
        }
        if total.is_some() || married.is_some() {
            Some(Self::new(total, married))
        } else {
            None
        }
    }

    /*
    fn calculate_from_patch_for_update_old<T>(value: usize, patch: Patch<T>, name: &str) -> Option<usize> {
        match patch {
            Patch::Value(_) => Some(value + 1),
            Patch::Null => Self::checked_decrement(value, name),
            Patch::Absent => None
        }
    }

    fn calculate_from_option_for_update_old<T>(value: usize, option: Option<T>, name: &str) -> Option<usize> {
        match option{
            Some(_) => Self::checked_decrement(value, name),
            None => None
        }
    }

    fn calculate_for_update_new<T>(value: usize, patch: Patch<T>, option: Option<T>) -> Option<usize> {
        match patch {
            Patch::Value(_) => Some(value + 1),
            Patch::Null => None,
            Patch::Absent => option.map(|_| value + 1)
        }
    }
    */

    fn checked_decrement(value: usize, name: &str) -> Option<usize> {
        if value == 0 {
            warn!("LocationAggregate.{} is already 0, no not decrement", name);
            None
        } else {
            Some(value - 1)
        }
    }

    /*
    pub fn of_add_person(aggr: &LocationData, person: &PersonData) -> Option<Self> {
        match person.location {
            Some(_) => {
                let total = Some(aggr.total + 1);
                let married = match person.spouse_id {
                    Some(_) => Some(aggr.married + 1),
                    None => None
                };
                Some(Self{ total, married })
            },
            None => None
        }
    }

    pub fn of_sub_person(aggr: &LocationData, person: &PersonData) -> Option<Self> {
        match person.location {
            Some(_) => {
                if aggr.total == 0 {
                    None
                } else {
                    let total = aggr.total - 1;
                    let married = match person.spouse_id {
                        Some(_) => Some(aggr.married - 1),
                        None => None
                    };
                    Some(Self{ total, married })
                }
            },
            None => None
        }
    }

    pub fn of_add_patch(aggr: &LocationData, patch: &PersonPatch) -> Option<Self> {
        match patch.location {
            Patch::Value(_) => {
                let total = aggr.total + 1;
                let married = match patch.spouse_id {
                    Patch::Value(_) => Some(aggr.married + 1),
                    Patch::Null => None,
                    Patch::Absent => None
                };
                Some(Self{ total, married })
            },
            Patch::Null => None,
            Patch::Absent => None
        }
    }

    pub fn of_sub_patch(aggr: &LocationData, patch: &PersonPatch) -> Option<Self> {
        match patch.location {
            Patch::Value(_) => {
                if aggr.total == 0 {
                    None
                } else {
                    let total = aggr.total - 1;
                    let married = match patch.spouse_id {
                        Patch::Value(_) => Some(aggr.married - 1),
                        Patch::Null => None,
                        Patch::Absent => None
                    };
                    Some(Self{ total, married })
                }
            },
            Patch::Null => None,
            Patch::Absent => None
        }
    }
    */
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
    // Tests for method for_update_old
    //

    #[test]
    pub fn test_for_update_old_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_old_keep_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_old_keep_location_remove_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Null);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(None, Some(2))));
    }

    #[test]
    pub fn test_for_update_old_keep_location_set_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Value(123));
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(None, Some(4))));
    }

    #[test]
    pub fn test_for_update_old_remove_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(0), Some(2))));
    }

    #[test]
    pub fn test_for_update_old_remove_location_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(0), None)));
    }

    #[test]
    pub fn test_for_update_old_remove_location_set_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Value(123));
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(0), None)));
    }

    #[test]
    pub fn test_for_update_old_remove_location_below_zero() {
        let aggr = LocationData::new(0, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_old_set_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_old_change_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(0), Some(2))));
    }

    #[test]
    pub fn test_for_update_old_change_location_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(0), None)));
    }

    #[test]
    pub fn test_for_update_old_change_location_set_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Value(123));
        let l_patch = LocationPatch::for_update_old(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(0), None)));
    }

    //
    // Tests for method for_update_new
    //

    #[test]
    pub fn test_for_update_new_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_new_keep_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_new_remove_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, None);
    }

    #[test]
    pub fn test_for_update_new_set_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    pub fn test_for_update_new_set_location_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, None);
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    pub fn test_for_update_new_set_location_remove_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, Some(124));
        let p_patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Null);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), None)));
    }

    #[test]
    pub fn test_for_update_new_change_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Absent);
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
        assert_eq!(l_patch, Some(LocationPatch::new(Some(2), Some(4))));
    }

    #[test]
    pub fn test_for_update_new_change_location_set_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let p_patch = PersonPatch::new(None, Patch::Value("There"), Patch::Value(123));
        let l_patch = LocationPatch::for_update_new(&aggr, &person, &p_patch);
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

    /*
    #[test]
    pub fn test_of_add_person() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let patch = LocationPatch::of_add_person(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(2, Some(4))));
    }

    #[test]
    pub fn test_of_add_person_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, None);
        let patch = LocationPatch::of_add_person(&aggr, &person);
        assert_eq!(patch, None);
    }

    #[test]
    pub fn test_of_add_person_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::of_add_person(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(2, None)));
    }

    #[test]
    pub fn test_of_sub_person() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let patch = LocationPatch::of_sub_person(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(0, Some(2))));
    }

    #[test]
    pub fn test_of_sub_person_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", None, None);
        let patch = LocationPatch::of_sub_person(&aggr, &person);
        assert_eq!(patch, None);
    }

    #[test]
    pub fn test_of_sub_person_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = LocationPatch::of_sub_person(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(0, None)));
    }

    #[test]
    pub fn test_of_sub_person_below_zero() {
        let aggr = LocationData::new(0, 0);
        let person = PersonData::new("Hans", Some("Here"), Some(123));
        let patch = LocationPatch::of_sub_person(&aggr, &person);
        assert_eq!(patch, None);
    }

    #[test]
    pub fn test_of_add_patch() {
        let aggr = LocationData::new(1, 3);
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123));
        let patch = LocationPatch::of_add_patch(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(2, Some(4))));
    }

    #[test]
    pub fn test_of_add_patch_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let patch = LocationPatch::of_add_patch(&aggr, &person);
        assert_eq!(patch, None);
    }

    #[test]
    pub fn test_of_add_patch_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let patch = LocationPatch::of_add_patch(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(2, None)));
    }

    #[test]
    pub fn test_of_sub_patch() {
        let aggr = LocationData::new(1, 3);
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123));
        let patch = LocationPatch::of_sub_patch(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(0, Some(2))));
    }

    #[test]
    pub fn test_of_sub_patch_no_location() {
        let aggr = LocationData::new(1, 3);
        let person = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        let patch = LocationPatch::of_sub_patch(&aggr, &person);
        assert_eq!(patch, None);
    }

    #[test]
    pub fn test_of_sub_patch_no_spouse() {
        let aggr = LocationData::new(1, 3);
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Absent);
        let patch = LocationPatch::of_sub_patch(&aggr, &person);
        assert_eq!(patch, Some(LocationPatch::new(0, None)));
    }

    #[test]
    pub fn test_of_sub_patch_below_zero() {
        let aggr = LocationData::new(0, 0);
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123));
        let patch = LocationPatch::of_sub_patch(&aggr, &person);
        assert_eq!(patch, None);
    }
    */
}
