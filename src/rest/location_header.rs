//
// The standard header axum::headers::Location apparently does not have a public constructor,
// so it cannot be used in responses. Thus a custom Location header is needed.
//

use std::convert::From;
use axum::headers::{Error, Header, HeaderName, HeaderValue};

static LOCATION_HEADER_NAME: HeaderName = HeaderName::from_static("location");

#[derive(Debug)]
pub struct LocationHeader(String);

impl From<String> for LocationHeader {
    fn from(item: String) -> Self {
        Self { 0: item }
    }
}

impl From<LocationHeader> for String {
    fn from(item: LocationHeader) -> Self {
        item.0
    }
}

impl Header for LocationHeader {
    fn name() -> &'static HeaderName {
        &LOCATION_HEADER_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, Error> where Self: Sized, I: Iterator<Item=&'i HeaderValue> {
        let value: &HeaderValue = values.next().ok_or_else(Error::invalid)?;
        let location = value.to_str().or_else(|_| Err(Error::invalid()))?;
        Ok(LocationHeader::from(location.to_string()))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let value = HeaderValue::from_str(self.0.as_str()).unwrap(); // TODO: Error handling?
        values.extend(std::iter::once(value));
    }
}

#[cfg(test)]
mod tests {
    use crate::rest::location_header::LocationHeader;
    use axum::headers::{Header, HeaderMap, HeaderMapExt};

    #[test]
    fn test_name() {
        assert_eq!(LocationHeader::name(), "location");
    }

    #[test]
    fn test_from() {
        let header = LocationHeader::from("foo".to_string());
        assert_eq!(String::from(header), "foo");
    }

    #[test]
    fn test_into() {
        let header : LocationHeader = "foo".to_string().into();
        let location : String = header.into();
        assert_eq!(location, "foo".to_string());
    }

    #[test]
    fn test_codec() {
        let mut map = HeaderMap::new();
        map.typed_insert(LocationHeader::from("foo".to_string())); // typed_insert calls encode
        let header : Option<LocationHeader> = map.typed_get(); // typed_get calls decode
        assert!(header.is_some());
        assert_eq!(String::from(header.unwrap()), "foo".to_string());
    }
}