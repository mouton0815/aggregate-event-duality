use std::collections::BTreeMap;
use serde::Serialize;
use crate::domain::person_event::PersonData;

#[derive(Serialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonMap {
    #[serde(default)]
    #[serde(flatten)]
    person_map: BTreeMap<u32, Option<PersonData>> // TODO: Keys in JSON are always strings
}

impl PersonMap {
    pub fn empty() -> Self {
        Self{ person_map: BTreeMap::new() }
    }

    pub fn of_one(entry: (u32, Option<PersonData>)) -> Self {
        let mut map = BTreeMap::new();
        map.insert(entry.0, entry.1);
        Self{ person_map: map }
    }

    pub fn of_two(entry1: (u32, Option<PersonData>), entry2: (u32, Option<PersonData>)) -> Self {
        let mut map = BTreeMap::new();
        map.insert(entry1.0, entry1.1);
        map.insert(entry2.0, entry2.1);
        Self{ person_map: map }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use serde::Serialize;
    use crate::domain::person_event::PersonData;
    use crate::domain::person_map::PersonMap;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_person_map() {
        let person_map = PersonMap::of_two(
            (1, Some(PersonData{
                name: Some("Hans".to_string()),
                location: Patch::Value("Berlin".to_string()),
                spouse_id: Patch::Value(2)
            })),
            (2, Some(PersonData{
                name: Some("Inge".to_string()),
                location: Patch::Value("Berlin".to_string()),
                spouse_id: Patch::Value(1)
            }))
        );

        let json_ref = r#"{"1":{"name":"Hans","location":"Berlin","spouseId":2},"2":{"name":"Inge","location":"Berlin","spouseId":1}}"#;
        serialize_and_verify(&person_map, json_ref);
    }

    #[test]
    pub fn test_person_map_mixed() {
        let person_map = PersonMap::of_two(
            (1, None),
            (2, Some(PersonData{
                name: Some("Inge".to_string()),
                location: Patch::Value("Berlin".to_string()),
                spouse_id: Patch::Absent
            }))
        );

        let json_ref = r#"{"1":null,"2":{"name":"Inge","location":"Berlin"}}"#;
        serialize_and_verify(&person_map, json_ref);
    }

    #[test]
    pub fn test_person_map_empty() {
        let person_map = PersonMap::empty();
        let json_ref = r#"{}"#;
        serialize_and_verify(&person_map, json_ref);
    }

    fn serialize_and_verify<PersonMap>(person_map_ref: &PersonMap, json_ref: &str)
        where PersonMap: Serialize + PartialEq + Debug {
        let json = serde_json::to_string(&person_map_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));
    }
}