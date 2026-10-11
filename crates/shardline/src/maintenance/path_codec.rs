//! Preserve native Unix paths while retaining the existing UTF-8 JSON representation.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    ffi::OsString,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum EncodedPath {
    Text(String),
    Native(NativePath),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativePath {
    unix_bytes: String,
}

impl EncodedPath {
    fn from_path(path: &Path) -> Self {
        path.to_str().map_or_else(
            || {
                Self::Native(NativePath {
                    unix_bytes: hex::encode(path.as_os_str().as_bytes()),
                })
            },
            |text| Self::Text(text.to_owned()),
        )
    }

    fn into_path<E: serde::de::Error>(self) -> Result<PathBuf, E> {
        match self {
            Self::Text(text) => Ok(PathBuf::from(text)),
            Self::Native(path) => {
                let bytes = hex::decode(path.unix_bytes).map_err(E::custom)?;
                if bytes.contains(&0) {
                    return Err(E::custom("native path contains a NUL byte"));
                }
                Ok(PathBuf::from(OsString::from_vec(bytes)))
            }
        }
    }
}

pub(super) fn serialize<S: Serializer>(path: &Path, serializer: S) -> Result<S::Ok, S::Error> {
    EncodedPath::from_path(path).serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PathBuf, D::Error> {
    EncodedPath::deserialize(deserializer)?.into_path()
}

pub(super) mod optional {
    use super::*;
    pub fn serialize<S: Serializer>(
        path: &Option<PathBuf>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        path.as_ref()
            .map(|path| EncodedPath::from_path(path))
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<PathBuf>, D::Error> {
        Option::<EncodedPath>::deserialize(deserializer)?
            .map(EncodedPath::into_path)
            .transpose()
    }
}
