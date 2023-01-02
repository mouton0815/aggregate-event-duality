use std::collections::HashMap;
use serde::{Deserialize,Serialize};
use crate::domain::location_patch::LocationPatch;

///
/// A location event. The encapsulated map always contains exactly one
/// [LocationPatch](crate::domain::location_patch::LocationPatch) object.
/// The implementation was chosen to produce the desired json output
/// <code>{ <location>: <location_data> }</code>.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct LocationEvent(HashMap<String, Option<LocationPatch>>);

impl LocationEvent {
    pub fn new(location: &str, patch: Option<LocationPatch>) -> Self {
        let mut map = HashMap::new();
        map.insert(location.to_string(), patch);
        Self{ 0: map }
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::location_event::LocationEvent;
    use crate::domain::location_patch::LocationPatch;
    use crate::util::serde_and_verify::tests::serde_and_verify;

    #[test]
    fn test_serde() {
        let patch = LocationPatch{ total: Some(1), married: Some(3) };
        let event = LocationEvent::new("Here", Some(patch));
        let json_ref = r#"{"Here":{"total":1,"married":3}}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    fn test_serde_null_object() {
        let event = LocationEvent::new("Here", None);
        let json_ref = r#"{"Here":null}"#;
        serde_and_verify(&event, json_ref);
    }

    #[test]
    fn test_serde_null_content() {
        let patch = LocationPatch{ total: None, married: None };
        let event = LocationEvent::new("Here", Some(patch));
        let json_ref = r#"{"Here":{}}"#;
        serde_and_verify(&event, json_ref);
    }
}