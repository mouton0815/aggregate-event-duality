use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompanyAggregate {
    pub company_id: u32,
    pub tenant_id: u32,
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub vat_id: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub employees: Option<u32>
}

#[cfg(test)]
mod tests {
    use crate::domain::company_aggregate::CompanyAggregate;

    #[test]
    pub fn test_serde_company_aggregate() {
        let company_ref = CompanyAggregate{
            company_id: 10,
            tenant_id: 1,
            name: String::from("Foo & Bar"),
            location: Some(String::from("Nowhere")),
            vat_id: None,
            employees: Some(75)
        };
        let json_ref = r#"{"companyId":10,"tenantId":1,"name":"Foo & Bar","location":"Nowhere","employees":75}"#;

        let json = serde_json::to_string(&company_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        let company: Result<CompanyAggregate, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(company.is_ok());
        assert_eq!(company.unwrap(), company_ref);
    }
}
