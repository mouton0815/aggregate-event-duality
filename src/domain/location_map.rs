use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::location_data::LocationData;

///
/// A map of [LocationData](crate::domain::location_data::LocationData) objects with their
/// locations as keys.
/// The implementation with an encapsulated map was chosen to produce the desired json output
/// <code>{ <location>: <location_data>, ... }</code>.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationMap(BTreeMap<String, LocationData>);

impl LocationMap {
    pub fn new() -> LocationMap {
        Self{ 0: BTreeMap::new() }
    }

    pub fn put(&mut self, name: &str, data: LocationData) {
        self.0.insert(name.to_string(),data);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, name: &str) -> &LocationData {
        self.0.get(name).unwrap() // Panic accepted
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_data::LocationData;
    use crate::domain::location_map::LocationMap;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_put() {
        let mut map = LocationMap::new();
        map.put("foo", LocationData{ total: 1, married: 3});
        map.put("bar", LocationData{ total: 2, married: 0});

        let json_ref = r#"{"bar":{"total":2,"married":0},"foo":{"total":1,"married":3}}"#;
        serde_and_verify(&map, json_ref);
    }

    #[test]
    pub fn test_empty() {
        let map = LocationMap::new();
        let json_ref = r#"{}"#;
        serde_and_verify(&map, json_ref);
    }

    #[test]
    pub fn test_get_and_len() {
        let loc = LocationData{ total: 1, married: 3};
        let mut map = LocationMap::new();
        map.put("foo", loc.clone());
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("foo"), &loc);
    }
}