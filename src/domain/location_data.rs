use serde::{Serialize, Deserialize};
use crate::domain::location_patch::LocationPatch;

///
/// ``LocationData`` represents statistical information (i.e. counters) about persons with respect
/// to a location. ``LocationData`` objects are store in
/// [LocationTable](crate::database::location_table::LocationTable).
///
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationData {
    pub total: usize,
    pub married: usize
}

impl LocationData {
    pub fn new(total: usize, married: usize) -> Self {
        Self { total, married }
    }

    pub fn apply_patch(&mut self, patch: &LocationPatch) {
        if let Some(value) = patch.total {
            self.total = value;
        }
        if let Some(value) = patch.married {
            self.married = value;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_data::LocationData;
    use crate::domain::location_patch::LocationPatch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    fn test_serde() {
        let data_ref = LocationData::new(1, 3);
        let json_ref = r#"{"total":1,"married":3}"#;
        serde_and_verify(&data_ref, json_ref);
    }

    #[test]
    fn test_apply_patch() {
        let mut loc = LocationData::new(1, 3);
        loc.apply_patch(&LocationPatch{ total: Some(2), married: Some(4) });
        assert_eq!(loc, LocationData::new(2, 4));
    }

    #[test]
    fn test_apply_patch_no_change() {
        let mut loc = LocationData::new(1, 3);
        loc.apply_patch(&LocationPatch{ total: None, married: None });
        assert_eq!(loc, LocationData::new(1, 3));
    }
}