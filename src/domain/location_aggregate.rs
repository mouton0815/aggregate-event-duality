use serde::{Serialize, Deserialize};
use crate::domain::person_aggregate::PersonAggregate;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LocationAggregate {
    pub name: String,
    pub persons: Vec<PersonAggregate>
}

#[cfg(test)]
mod tests {
    use crate::domain::location_aggregate::LocationAggregate;
    use crate::domain::person_aggregate::PersonAggregate;

    #[test]
    pub fn test_location_aggregate_empty() {
        let location_ref = LocationAggregate {
            name: "Berlin".to_string(),
            persons: vec![]
        };
        let json_ref = r#"{"name":"Berlin","persons":[]}"#;

        let json = serde_json::to_string(&location_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let location: Result<LocationAggregate, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(location.is_ok());
        assert_eq!(location.unwrap(), location_ref);
    }

    #[test]
    pub fn test_location_aggregate() {
        let location_ref = LocationAggregate {
            name: "Berlin".to_string(),
            persons: vec![
                PersonAggregate{
                    person_id: 1,
                    name: "Hans".to_string(),
                    location: Some("Nowhere".to_string()),
                    spouse_id: None
                },
                PersonAggregate{
                    person_id: 2,
                    name: "Inge".to_string(),
                    location: None,
                    spouse_id: Some(5)
                }
            ]
        };
        let json_ref = r#"{"name":"Berlin","persons":[{"personId":1,"name":"Hans","location":"Nowhere"},{"personId":2,"name":"Inge","spouseId":5}]}"#;

        let json = serde_json::to_string(&location_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let location: Result<LocationAggregate, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(location.is_ok());
        assert_eq!(location.unwrap(), location_ref);
    }
}
