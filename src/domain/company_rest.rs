use serde::{Serialize, Deserialize};
use crate::util::patch::Patch;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompanyPost {
    pub tenant_id: u32,
    pub name: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vat_id: Option<u32>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub employees: Option<u32>,
}

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
    use crate::domain::company_rest::{CompanyPost, CompanyPut};
    use crate::util::patch::Patch;

    #[test]
    pub fn test_serde_company_post() {
        let company_ref = CompanyPost {
            tenant_id: 10,
            name: String::from("Foo & Bar"),
            location: None,
            vat_id: None,
            employees: Some(75)
        };
        let json_ref = r#"{"tenantId":10,"name":"Foo & Bar","employees":75}"#;

        let json = serde_json::to_string(&company_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let company: Result<CompanyPost, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(company.is_ok());
        assert_eq!(company.unwrap(), company_ref);
    }

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
