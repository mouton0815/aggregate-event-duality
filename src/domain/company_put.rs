// TODO: Rather patch?

use serde::{Serialize, Deserialize};
use crate::patch::Patch;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompanyPut {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<u32>, // tenant_id can be updated or left as is, but not deleted

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>, // name can be updated or left as is, but not deleted

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub location: Patch<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub vat_id: Patch<u32>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub employees: Patch<u32>,
}

#[cfg(test)]
mod tests {
    use crate::domain::company_put::CompanyPut;
    use crate::patch::Patch;

    #[test]
    pub fn test_serde_company_create_event() {
        let company_ref = CompanyPut {
            tenant_id: Some(10),
            name: Some(String::from("Foo & Bar")),
            location: Patch::Absent,
            vat_id: Patch::Null,
            employees: Patch::Value(75)
        };
        let json_ref = r#"{"tenantId":10,"name":"Foo & Bar","vatId":null,"employees":75}"#;

        let json = serde_json::to_string(&company_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let company: Result<CompanyPut, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(company.is_ok());
        assert_eq!(company.unwrap(), company_ref);
    }
}
