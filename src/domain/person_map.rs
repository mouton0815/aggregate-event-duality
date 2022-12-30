use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_data::PersonData;

///
/// A map of [PersonData](crate::domain::person_data::PersonData) objects with their ids as keys.
/// The implementation with an encapsulated map was chosen to produce the desired json output
/// <code>{ <person_id>: <person_data>, ... }</code>.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonMap(BTreeMap<u32, PersonData>); // TODO: Keys in JSON are always strings

impl PersonMap {
    pub fn new() -> Self {
        Self{ 0: BTreeMap::new() }
    }

    pub fn put(&mut self, person_id: u32, person_data: PersonData) {
        self.0.insert(person_id, person_data);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, person_id: u32) -> &PersonData {
        self.0.get(&person_id).unwrap() // Panic accepted
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_put() {
        let mut map = PersonMap::new();
        map.put(1, PersonData{
            name: "Hans".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: Some(2)
        });
        map.put(2, PersonData{
            name: "Inge".to_string(),
            location: Some("Berlin".to_string()),
            spouse_id: Some(1)
        });

        let json_ref = r#"{"1":{"name":"Hans","location":"Berlin","spouseId":2},"2":{"name":"Inge","location":"Berlin","spouseId":1}}"#;
        serde_and_verify(&map, json_ref);
    }

    #[test]
    pub fn test_empty() {
        let map = PersonMap::new();
        let json_ref = r#"{}"#;
        serde_and_verify(&map, json_ref);
    }

    #[test]
    pub fn test_get_and_len() {
        let person = PersonData{
            name: "Inge".to_string(),
            location: Some("Nowhere".to_string()),
            spouse_id: None
        };
        let mut map = PersonMap::new();
        map.put(5, person.clone());
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(5), &person);
    }
}