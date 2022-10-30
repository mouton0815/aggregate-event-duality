use serde::{Serialize, Deserialize};

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

#[cfg(test)]
mod tests {
    use crate::domain::company_post::CompanyPost;

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
}
