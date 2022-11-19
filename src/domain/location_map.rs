use std::collections::BTreeMap;
use serde::Serialize;
use crate::domain::person_map::PersonMap;

#[derive(Serialize, Debug, Eq, PartialEq)]
pub struct LocationMap(BTreeMap<String, Option<PersonMap>>);

impl LocationMap {
    pub fn new() -> LocationMap {
        Self{ 0: BTreeMap::new() }
    }

    /*
    pub fn of(location: &str, person_map: Option<PersonMap>) -> Self {
        let mut map = BTreeMap::new();
        map.insert(location.to_string(), person_map);
        Self{ 0: map }
    }
    */
    pub fn put(&mut self, location: &str, persons: Option<PersonMap>) {
        self.0.insert(location.to_string(), persons);
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use serde::Serialize;
    use crate::domain::location_map::{LocationMap, PersonMap};
    use crate::domain::person_event::PersonData;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_location_map() {
        let mut person_map = PersonMap::new();
        person_map.put(1, Some(PersonData {
            name: Some("Hans".to_string()),
            location: Patch::Value("Berlin".to_string()),
            spouse_id: Patch::Null
        }));

        let mut location_map = LocationMap::new();
        location_map.put("Berlin", Some(person_map));
        let json_ref = r#"{"Berlin":{"1":{"name":"Hans","location":"Berlin","spouseId":null}}}"#;
        serialize_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_empty_persons() {
        let person_map = PersonMap::new();
        let mut location_map = LocationMap::new();
        location_map.put("Berlin", Some(PersonMap::new()));
        let json_ref = r#"{"Berlin":{}}"#;
        serialize_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_null_persons() {
        let mut location_map = LocationMap::new();
        location_map.put("Berlin", None);
        let json_ref = r#"{"Berlin":null}"#;
        serialize_and_verify(&location_map, json_ref);
    }

    #[test]
    pub fn test_location_map_empty() {
        let location_map = LocationMap::new();
        let json_ref = r#"{}"#;
        serialize_and_verify(&location_map, json_ref);
    }

    fn serialize_and_verify<LocationMap>(location_map_ref: &LocationMap, json_ref: &str)
        where LocationMap: Serialize + PartialEq + Debug {
        let json = serde_json::to_string(&location_map_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));
    }
}