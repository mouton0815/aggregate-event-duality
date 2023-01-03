use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
use crate::domain::person_data::PersonData;
use crate::domain::person_id::PersonId;

///
/// A map of [PersonData](crate::domain::person_data::PersonData) objects with their ids as keys.
/// The implementation with an encapsulated map was chosen to produce the desired json output
/// <code>{ <person_id>: <person_data>, ... }</code>.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonMap(BTreeMap<PersonId, PersonData>);

impl PersonMap {
    pub fn new() -> Self {
        Self{ 0: BTreeMap::new() }
    }

    pub fn put(&mut self, person_id: PersonId, person_data: PersonData) {
        self.0.insert(person_id, person_data);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, person_id: PersonId) -> &PersonData {
        self.0.get(&person_id).unwrap() // Panic accepted
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_id::PersonId;
    use crate::domain::person_map::PersonMap;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    fn test_put() {
        let mut map = PersonMap::new();
        map.put(PersonId::from(1), PersonData::new("Ann", Some("here"), Some(PersonId::from(2))));
        map.put(PersonId::from(2), PersonData::new("Bob", None, Some(PersonId::from(1))));

        let json_ref = r#"{"1":{"name":"Ann","city":"here","spouse":2},"2":{"name":"Bob","spouse":1}}"#;
        serde_and_verify(&map, json_ref);
    }

    #[test]
    fn testest_empty() {
        let map = PersonMap::new();
        let json_ref = r#"{}"#;
        serde_and_verify(&map, json_ref);
    }

    #[test]
    fn testest_get_and_len() {
        let person = PersonData::new("Bob", Some("nowhere"), None);
        let mut map = PersonMap::new();
        map.put(PersonId::from(5), person.clone());
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(PersonId::from(5)), &person);
    }
}