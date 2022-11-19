use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonData {
    pub name: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spouse_id: Option<u32>
}

#[cfg(test)]
mod tests {
    use crate::domain::person_data::PersonData;

    #[test]
    pub fn test_person_data() {
        let person_ref = PersonData {
            name: String::from("Hans"),
            location: None,
            spouse_id: Some(2)
        };
        let json_ref = r#"{"name":"Hans","spouseId":2}"#;

        let json = serde_json::to_string(&person_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let person: Result<PersonData, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person.is_ok());
        assert_eq!(person.unwrap(), person_ref);
    }
}

