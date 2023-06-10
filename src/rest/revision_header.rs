use std::convert::From;
use axum::headers::{Error, Header, HeaderName, HeaderValue};

static REVISION_HEADER_NAME: HeaderName = HeaderName::from_static("x-revision");

// TODO: Change to u64
#[derive(Debug)]
pub struct RevisionHeader(usize);

impl From<usize> for RevisionHeader {
    fn from(item: usize) -> Self {
        Self { 0: item }
    }
}

impl From<RevisionHeader> for usize {
    fn from(item: RevisionHeader) -> Self {
        item.0
    }
}

impl Header for RevisionHeader {
    fn name() -> &'static HeaderName {
        &REVISION_HEADER_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, Error> where Self: Sized, I: Iterator<Item=&'i HeaderValue> {
        let value: &HeaderValue = values.next().ok_or_else(Error::invalid)?;
        let revision = value.to_str().or_else(|_| Err(Error::invalid()))?;
        let revision : usize = revision.trim().parse().or_else(|_| Err(Error::invalid()))?;
        Ok(RevisionHeader::from(revision))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let value = HeaderValue::from(self.0);
        values.extend(std::iter::once(value));
    }
}

#[cfg(test)]
mod tests {
    use crate::rest::revision_header::RevisionHeader;
    use axum::headers::{Header, HeaderMap, HeaderMapExt};

    #[test]
    fn test_name() {
        assert_eq!(RevisionHeader::name(), "x-revision");
    }

    #[test]
    fn test_from() {
        let header = RevisionHeader::from(3);
        assert_eq!(usize::from(header), 3);
    }

    #[test]
    fn test_into() {
        let header : RevisionHeader = 5.into();
        let revision : usize = header.into();
        assert_eq!(revision, 5);
    }

    /*
    #[test]
    fn test_decode() {
        let mut map = HeaderMap::new();
        map.append(RevisionHeader::name(), HeaderValue::from(7));
        let mut values = map.get_all(RevisionHeader::name()).iter();
        let header = RevisionHeader::decode(&mut values);
        assert!(header.is_ok());
        assert_eq!(usize::from(header.unwrap()), 7);
    }
    */

    #[test]
    fn test_codec() {
        let mut map = HeaderMap::new();
        map.typed_insert(RevisionHeader::from(7)); // typed_insert calls encode
        let header : Option<RevisionHeader> = map.typed_get(); // typed_get calls decode
        assert!(header.is_some());
        assert_eq!(usize::from(header.unwrap()), 7);
    }
}