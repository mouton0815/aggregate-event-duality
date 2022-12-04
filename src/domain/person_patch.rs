use serde::{Serialize, Deserialize};
use crate::domain::person_data::PersonData;
use crate::util::patch::Patch;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonPatch {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>, // name can be updated or left as is, but not deleted

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub location: Patch<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub spouse_id: Patch<u32>
}

impl PersonPatch {
    /// Convenience function that takes &str literals
    pub fn new(name: Option<&str>, location: Patch<&str>, spouse_id: Patch<u32>) -> Self {
        PersonPatch {
            name: name.map(|n| String::from(n)),
            location: location.map(|l| String::from(l)),
            spouse_id
        }
    }

    pub fn of(old: &PersonData, new: &PersonData) -> Self {
        let name = if old.name == new.name { None } else { Some(new.name.clone()) };
        let location = Patch::of(&old.location, &new.location);
        let spouse_id = Patch::of(&old.spouse_id, &new.spouse_id);
        PersonPatch{ name, location, spouse_id }
    }

    pub fn is_noop(&self) -> bool {
        self.name.is_none() && self.location.is_absent() && self.spouse_id.is_absent()
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_serde1() {
        let person = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Null);
        let json_ref = r#"{"name":"Hans","spouseId":null}"#;
        serde_and_verify(&person, json_ref);
    }

    #[test]
    pub fn test_serde2() {
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123));
        let json_ref = r#"{"location":"Here","spouseId":123}"#;
        serde_and_verify(&person, json_ref);
    }

    #[test]
    pub fn test_serde3() {
        let person = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let json_ref = r#"{"location":null}"#;
        serde_and_verify(&person, json_ref);
    }

    #[test]
    pub fn test_of1() {
        let old = PersonData::new("Hans", None, None);
        let new = PersonData::new("Inge", Some("here"), None);
        let cmp = PersonPatch::new(Some("Inge"), Patch::Value("here"), Patch::Absent);
        assert_eq!(PersonPatch::of(&old, &new), cmp);
    }

    #[test]
    pub fn test_of2() {
        let old = PersonData::new("Hans", Some("here"), Some(123));
        let new = PersonData::new("Hans", None, None);
        let cmp = PersonPatch::new(None, Patch::Null, Patch::Null);
        assert_eq!(PersonPatch::of(&old, &new), cmp);
    }

    #[test]
    pub fn test_noop1() {
        let person = PersonPatch::new(None, Patch::Value("foo"), Patch::Absent);
        assert_eq!(person.is_noop(), false);
    }

    #[test]
    pub fn test_noop2() {
        let person = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Absent);
        assert_eq!(person.is_noop(), false);
    }

    #[test]
    pub fn test_noop3() {
        let person = PersonPatch::new(None, Patch::Absent, Patch::Value(123));
        assert_eq!(person.is_noop(), false);
    }

    #[test]
    pub fn test_noop4() {
        let person = PersonPatch::new(None, Patch::Null, Patch::Absent);
        assert_eq!(person.is_noop(), false);
    }

    #[test]
    pub fn test_noop5() {
        let person = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        assert_eq!(person.is_noop(), true);
    }

    fn serde_and_verify(person_ref: &PersonPatch, json_ref: &str) {
        // 1. Serialize person_ref and string-compare it to json_ref
        let json = serde_json::to_string(&person_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with person_ref
        let person: Result<PersonPatch, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person.is_ok());
        assert_eq!(person.unwrap(), *person_ref);
    }
}
