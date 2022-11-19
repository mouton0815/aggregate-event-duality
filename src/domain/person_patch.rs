use serde::{Serialize, Deserialize};
use crate::util::patch::Patch;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
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

#[cfg(test)]
mod tests {
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_person_patch() {
        let person_ref = PersonPatch {
            name: Some(String::from("Hans")),
            location: Patch::Absent,
            spouse_id: Patch::Null,
        };
        let json_ref = r#"{"name":"Hans","spouseId":null}"#;

        let json = serde_json::to_string(&person_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let person: Result<PersonPatch, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person.is_ok());
        assert_eq!(person.unwrap(), person_ref);
    }
}
