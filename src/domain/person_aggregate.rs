use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersonAggregate {
    pub person_id: u32,
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spouse_id: Option<u32>
}

#[cfg(test)]
mod tests {
    use crate::domain::person_aggregate::PersonAggregate;

    #[test]
    pub fn test_person_aggregate() {
        let person_ref = PersonAggregate {
            person_id: 10,
            name: String::from("Hans"),
            location: Some(String::from("Nowhere")),
            spouse_id: None
        };
        let json_ref = r#"{"personId":10,"name":"Hans","location":"Nowhere"}"#;

        let json = serde_json::to_string(&person_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let person: Result<PersonAggregate, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(person.is_ok());
        assert_eq!(person.unwrap(), person_ref);
    }
}
