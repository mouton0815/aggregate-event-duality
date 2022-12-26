use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_map::PersonMap;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationMap(BTreeMap<String, PersonMap>);

impl LocationMap {
    pub fn new() -> LocationMap {
        Self{ 0: BTreeMap::new() }
    }

    pub fn put(&mut self, location: &str, persons: PersonMap) {
        self.0.insert(location.to_string(), persons);
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_map::LocationMap;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_location_map() {
        let mut person_map = PersonMap::new();
        person_map.put(1, PersonData {
            name: "Hans".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: None
        });

        let mut location_map = LocationMap::new();
        location_map.put("Berlin", person_map);
        let json_ref = r#"{"Berlin":{"1":{"name":"Hans","location":"Berlin"}}}"#;
        serde_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_empty_persons() {
        let mut location_map = LocationMap::new();
        location_map.put("Berlin", PersonMap::new());
        let json_ref = r#"{"Berlin":{}}"#;
        serde_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_empty() {
        let location_map = LocationMap::new();
        let json_ref = r#"{}"#;
        serde_and_verify(&location_map, json_ref);
    }
}