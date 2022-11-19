use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_map::PersonMap;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationMap(BTreeMap<String, Option<PersonMap>>);

impl LocationMap {
    pub fn new() -> LocationMap {
        Self{ 0: BTreeMap::new() }
    }

    pub fn put(&mut self, location: &str, persons: Option<PersonMap>) {
        self.0.insert(location.to_string(), persons);
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_map::LocationMap;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;

    #[test]
    pub fn test_location_map() {
        let mut person_map = PersonMap::new();
        person_map.put(1, Some(PersonData {
            name: "Hans".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: None
        }));

        let mut location_map = LocationMap::new();
        location_map.put("Berlin", Some(person_map));
        let json_ref = r#"{"Berlin":{"1":{"name":"Hans","location":"Berlin"}}}"#;
        serde_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_empty_persons() {
        let mut location_map = LocationMap::new();
        location_map.put("Berlin", Some(PersonMap::new()));
        let json_ref = r#"{"Berlin":{}}"#;
        serde_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_null_persons() {
        let mut location_map = LocationMap::new();
        location_map.put("Berlin", None);
        let json_ref = r#"{"Berlin":null}"#;
        serde_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_empty() {
        let location_map = LocationMap::new();
        let json_ref = r#"{}"#;
        serde_and_verify(&location_map, json_ref);
    }


    fn serde_and_verify(location_map_ref: &LocationMap, json_ref: &str) {
        // 1. Serialize location_map_ref and string-compare it to json_ref
        let json = serde_json::to_string(&location_map_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with location_map_ref
        let location_map: Result<LocationMap, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(location_map.is_ok());
        assert_eq!(location_map.unwrap(), *location_map_ref);
    }
}