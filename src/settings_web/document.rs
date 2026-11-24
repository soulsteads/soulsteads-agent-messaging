use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// Keep scalar JSON literals intact when rewriting a document. The shared
/// strict parser checks syntax, duplicate keys and depth before this is used.
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Document<'a> {
    Object(BTreeMap<String, Document<'a>>),
    Array(Vec<Document<'a>>),
    Scalar(&'a RawValue),
}

impl<'a> Document<'a> {
    pub(super) fn parse(input: &'a str) -> serde_json::Result<Self> {
        let raw = serde_json::from_str::<&RawValue>(input)?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: &'a RawValue) -> serde_json::Result<Self> {
        let input = raw.get();
        match input.as_bytes().first() {
            Some(b'{') => serde_json::from_str::<BTreeMap<String, &RawValue>>(input)?
                .into_iter()
                .map(|(key, value)| Ok((key, Self::from_raw(value)?)))
                .collect::<serde_json::Result<_>>()
                .map(Self::Object),
            Some(b'[') => serde_json::from_str::<Vec<&RawValue>>(input)?
                .into_iter()
                .map(Self::from_raw)
                .collect::<serde_json::Result<_>>()
                .map(Self::Array),
            _ => Ok(Self::Scalar(raw)),
        }
    }
}

impl PartialEq for Document<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Object(a), Self::Object(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Scalar(a), Self::Scalar(b)) => a.get() == b.get(),
            _ => false,
        }
    }
}
