use serde::{Serialize, Deserialize};
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationData {
    pub total: usize,
    pub married: usize
}

impl LocationData {
    pub fn new(total: usize, married: usize) -> Self {
        Self { total, married }
    }

    pub fn add_person(&mut self, person: &PersonData) {
        if person.location.is_some() {
            self.total += 1;
            if person.spouse_id.is_some() {
                self.married += 1;
            }
        }
    }

    pub fn sub_person(&mut self, person: &PersonData) {
        if person.location.is_some() && self.total > 0 {
            self.total -= 1;
            if person.spouse_id.is_some() && self.married > 0 {
                self.married -= 1;
            }
        }
    }

    pub fn add_patch(&mut self, patch: &PersonPatch) {
        if patch.location.is_value() {
            self.total += 1;
            if patch.spouse_id.is_value() {
                self.married += 1;
            }
        }
    }

    pub fn sub_patch(&mut self, patch: &PersonPatch) {
        if patch.location.is_value() && self.total > 0 {
            self.total -= 1;
            if patch.spouse_id.is_value() && self.married > 0 {
                self.married -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_data::LocationData;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_serde() {
        let data_ref = LocationData::new(1, 3);
        let json_ref = r#"{"total":1,"married":3}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    #[test]
    pub fn test_add_person() {
        let mut loc = LocationData::new(1, 3);
        loc.add_person(&PersonData::new("Hans", Some("Here"), Some(123)));
        assert_eq!(loc, LocationData::new(2, 4));
    }

    #[test]
    pub fn test_add_person_no_location() {
        let mut loc = LocationData::new(1, 3);
        loc.add_person(&PersonData::new("Hans", None, Some(123)));
        assert_eq!(loc, LocationData::new(1, 3));
    }

    #[test]
    pub fn test_add_person_no_spouse() {
        let mut loc = LocationData::new(1, 3);
        loc.add_person(&PersonData::new("Hans", Some("Here"), None));
        assert_eq!(loc, LocationData::new(2, 3));
    }

    #[test]
    pub fn test_sub_person() {
        let mut loc = LocationData::new(1, 3);
        loc.sub_person(&PersonData::new("Hans", Some("Here"), Some(123)));
        assert_eq!(loc, LocationData::new(0, 2));
    }

    #[test]
    pub fn test_sub_person_no_location() {
        let mut loc = LocationData::new(1, 3);
        loc.sub_person(&PersonData::new("Hans", None, Some(123)));
        assert_eq!(loc, LocationData::new(1, 3));
    }

    #[test]
    pub fn test_sub_person_no_spouse() {
        let mut loc = LocationData::new(1, 3);
        loc.sub_person(&PersonData::new("Hans", Some("Here"), None));
        assert_eq!(loc, LocationData::new(0, 3));
    }

    #[test]
    pub fn test_sub_person_below_zero() {
        let mut loc = LocationData::new(0, 0);
        loc.sub_person(&PersonData::new("Hans", Some("Here"), Some(123)));
        assert_eq!(loc, LocationData::new(0, 0));
    }

    #[test]
    pub fn test_add_patch() {
        let mut loc = LocationData::new(1, 3);
        loc.add_patch(&PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123)));
        assert_eq!(loc, LocationData::new(2, 4));
    }

    #[test]
    pub fn test_add_patch_no_location() {
        let mut loc = LocationData::new(1, 3);
        loc.add_patch(&PersonPatch::new(None, Patch::Absent, Patch::Absent));
        assert_eq!(loc, LocationData::new(1, 3));
    }

    #[test]
    pub fn test_add_patch_no_spouse() {
        let mut loc = LocationData::new(1, 3);
        loc.add_patch(&PersonPatch::new(None, Patch::Value("Here"), Patch::Absent));
        assert_eq!(loc, LocationData::new(2, 3));
    }

    #[test]
    pub fn test_sub_patch() {
        let mut loc = LocationData::new(1, 3);
        loc.sub_patch(&PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123)));
        assert_eq!(loc, LocationData::new(0, 2));
    }

    #[test]
    pub fn test_sub_patch_no_location() {
        let mut loc = LocationData::new(1, 3);
        loc.sub_patch(&PersonPatch::new(None, Patch::Absent, Patch::Absent));
        assert_eq!(loc, LocationData::new(1, 3));
    }

    #[test]
    pub fn test_sub_patch_no_spouse() {
        let mut loc = LocationData::new(1, 3);
        loc.sub_patch(&PersonPatch::new(None, Patch::Value("Here"), Patch::Absent));
        assert_eq!(loc, LocationData::new(0, 3));
    }

    #[test]
    pub fn test_sub_patch_below_zero() {
        let mut loc = LocationData::new(0, 0);
        loc.sub_patch(&PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123)));
        assert_eq!(loc, LocationData::new(0, 0));
    }
}