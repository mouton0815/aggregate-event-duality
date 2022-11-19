use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_data::PersonData;

/// A map of persons with their ids as keys.
/// The implementation with an encapsulated map was chosen to produce the desired json output
/// <code>{ <person_id>: <person_data>, ... }</code>.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonMap(BTreeMap<u32, Option<PersonData>>); // TODO: Keys in JSON are always strings

impl PersonMap {
    pub fn new() -> Self {
        Self{ 0: BTreeMap::new() }
    }

    /*
    pub fn of(entry: (u32, Option<PersonData>)) -> Self {
        let mut map = BTreeMap::new();
        map.insert(entry.0, entry.1);
        Self{ 0: map }
    }
    */

    pub fn put(&mut self, person_id: u32, person_data: Option<PersonData>) {
        self.0.insert(person_id, person_data);
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use serde::{Deserialize, Serialize};
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;

    #[test]
    pub fn test_person_map() {
        let mut person_map = PersonMap::new();
        person_map.put(1, Some(PersonData{
            name: "Hans".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: Some(2)
        }));
        person_map.put(2, Some(PersonData{
            name: "Inge".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: Some(1)
        }));

        let json_ref = r#"{"1":{"name":"Hans","location":"Berlin","spouseId":2},"2":{"name":"Inge","location":"Berlin","spouseId":1}}"#;
        serde_and_verify(&person_map, json_ref);
    }

    #[test]
    pub fn test_person_map_mixed() {
        let mut person_map = PersonMap::new();
        person_map.put(1, None);
        person_map.put(2, Some(PersonData{
            name: "Inge".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: None
        }));

        let json_ref = r#"{"1":null,"2":{"name":"Inge","location":"Berlin"}}"#;
        serde_and_verify(&person_map, json_ref);
    }

    #[test]
    pub fn test_person_map_empty() {
        let person_map = PersonMap::new();
        let json_ref = r#"{}"#;
        serde_and_verify(&person_map, json_ref);
    }

    fn serde_and_verify<'a, PersonMap>(person_map_ref: &PersonMap, json_ref: &'a str)
        where PersonMap: Serialize + Deserialize<'a> + PartialEq + Debug {

        // 1. Serialize person_map_ref and string-compare it to json_ref
        let json = serde_json::to_string(&person_map_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with person_map_ref
        let person_map: Result<PersonMap, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person_map.is_ok());
        assert_eq!(person_map.unwrap(), *person_map_ref);
    }
}