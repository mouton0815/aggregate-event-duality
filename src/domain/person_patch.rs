use serde::{Serialize, Deserialize};
use crate::domain::person_data::PersonData;
use crate::util::patch::Patch;

///
/// Changes of person data as received via ``PATCH`` requests.
///
/// ``PersonPatch`` objects are also the body of person events.
/// A [PersonEvent](crate::domain::person_event::PersonEvent) represents changes of a person
/// (insert, update, delete).
/// A serialized ``PersonEvent`` contains only fields that changed, all others are left out.
/// This is modeled with [Option](core::option) and [Patch](crate::util::patch::Patch) wrappers.
///
/// ``PersonPatch`` objects are constructed from
/// [PersonData](crate::domain::person_data::PersonData) objects.
///
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct PersonPatch {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>, // name can be updated or left as is, but not deleted

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub city: Patch<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub spouse: Patch<u32>
}

impl PersonPatch {
    /// Convenience function that takes &str literals
    pub fn new(name: Option<&str>, city: Patch<&str>, spouse: Patch<u32>) -> Self {
        Self {
            name: name.map(|n| String::from(n)),
            city: city.map(|l| String::from(l)),
            spouse
        }
    }

    pub fn of(old: &PersonData, new: &PersonData) -> Option<Self> {
        let name = if old.name == new.name { None } else { Some(new.name.clone()) };
        let city = Patch::of_options(&old.city, &new.city);
        let spouse = Patch::of_options(&old.spouse, &new.spouse);
        if name.is_none() && city.is_absent() && spouse.is_absent() {
            None
        } else {
            Some(Self{ name, city, spouse })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    pub fn test_serde1() {
        let person = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Null);
        let json_ref = r#"{"name":"Hans","spouse":null}"#;
        serde_and_verify(&person, json_ref);
    }

    #[test]
    pub fn test_serde2() {
        let person = PersonPatch::new(None, Patch::Value("Here"), Patch::Value(123));
        let json_ref = r#"{"city":"Here","spouse":123}"#;
        serde_and_verify(&person, json_ref);
    }

    #[test]
    pub fn test_serde3() {
        let person = PersonPatch::new(None, Patch::Null, Patch::Absent);
        let json_ref = r#"{"city":null}"#;
        serde_and_verify(&person, json_ref);
    }

    #[test]
    pub fn test_of1() {
        let old = PersonData::new("Hans", None, None);
        let new = PersonData::new("Inge", Some("here"), None);
        let cmp = PersonPatch::new(Some("Inge"), Patch::Value("here"), Patch::Absent);
        assert_eq!(PersonPatch::of(&old, &new), Some(cmp));
    }

    #[test]
    pub fn test_of2() {
        let old = PersonData::new("Hans", Some("here"), Some(123));
        let new = PersonData::new("Hans", None, None);
        let cmp = PersonPatch::new(None, Patch::Null, Patch::Null);
        assert_eq!(PersonPatch::of(&old, &new), Some(cmp));
    }

    #[test]
    pub fn test_of3() {
        let old = PersonData::new("Hans", Some("here"), Some(123));
        let new = PersonData::new("Hans", Some("here"), Some(123));
        assert_eq!(PersonPatch::of(&old, &new), None);
    }

    #[test]
    pub fn test_of4() {
        let old = PersonData::new("", None, None);
        let new = PersonData::new("", None, None);
        assert_eq!(PersonPatch::of(&old, &new), None);
    }
}
