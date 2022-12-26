use serde::{Serialize, Deserialize};
use crate::domain::location_data::LocationData;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationPatch {
    pub total: usize,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub married: Option<usize> // Attribute can be updated or left as is, but not deleted
}

impl LocationPatch {
    pub fn new(total: usize, married: Option<usize>) -> Self {
        Self { total, married }
    }

    pub fn of_add_person(aggr: &LocationData, person: &PersonData) -> Option<Self> {
        match person.location {
            Some(_) => {
                let total = aggr.total + 1;
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
}

#[cfg(test)]
mod tests {
    use crate::domain::location_data::LocationData;
    use crate::domain::location_patch::LocationPatch;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_serde_some() {
        let data_ref = LocationPatch::new(1, Some(3));
        let json_ref = r#"{"total":1,"married":3}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    #[test]
    pub fn test_serde_none() {
        let data_ref = LocationPatch::new(1, None);
        let json_ref = r#"{"total":1}"#;
        serde_and_verify(&data_ref, json_ref);
    }

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
}
