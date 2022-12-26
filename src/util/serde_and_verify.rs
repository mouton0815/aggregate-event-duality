#[cfg(test)]
pub mod tests {
    use std::fmt::Debug;
    use serde::{Deserialize, Serialize};

    pub fn serde_and_verify<'de, Data>(data_ref: &Data, json_ref: &'de str)
        where Data: Debug + Deserialize<'de> + Serialize + PartialEq {

        // 1. Serialize data_ref and string-compare it to json_ref
        let json = serde_json::to_string(&data_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with data_ref
        let data: Result<Data, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), *data_ref);
    }
}