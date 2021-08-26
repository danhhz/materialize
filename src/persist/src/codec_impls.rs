// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of [Codec] for stdlib types.

use crate::Codec;

use std::convert::TryFrom;

impl Codec for () {
    fn codec_name() -> &'static str {
        "()"
    }

    fn size_hint(&self) -> usize {
        0
    }

    fn encode<E: for<'a> Extend<&'a u8>>(&self, _buf: &mut E) {
        // No-op.
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        if !buf.is_empty() {
            return Err(format!("decode expected empty buf got {} bytes", buf.len()));
        }
        Ok(())
    }
}

impl Codec for String {
    fn codec_name() -> &'static str {
        "String"
    }

    fn size_hint(&self) -> usize {
        self.as_bytes().len()
    }

    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
        buf.extend(self.as_bytes())
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())
    }
}

impl Codec for Vec<u8> {
    fn codec_name() -> &'static str {
        "Vec<u8>"
    }

    fn size_hint(&self) -> usize {
        self.len()
    }

    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
        buf.extend(self)
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        Ok(buf.to_owned())
    }
}

impl<K: Codec, V: Codec> Codec for (K, V) {
    fn codec_name() -> &'static str {
        "Tuple2"
    }

    fn size_hint(&self) -> usize {
        self.0.size_hint() + self.1.size_hint() + 8 + 8
    }

    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
        let mut inner_buf = Vec::new();
        self.0.encode(&mut inner_buf);
        buf.extend(&inner_buf.len().to_le_bytes());
        buf.extend(&inner_buf);

        inner_buf.clear();
        self.1.encode(&mut inner_buf);
        buf.extend(&inner_buf.len().to_le_bytes());
        buf.extend(&inner_buf);
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let key_len_bytes = &buf[0..8];
        let key_len_bytes = <[u8; 8]>::try_from(key_len_bytes).map_err(|err| err.to_string())?;
        let key_end: usize = 8 + usize::from_le_bytes(key_len_bytes);
        let key_slice = &buf[8..key_end];
        let key = K::decode(key_slice)?;

        let value_len_bytes = &buf[(key_end)..(key_end + 8)];
        let value_len_bytes =
            <[u8; 8]>::try_from(value_len_bytes).map_err(|err| err.to_string())?;
        let value_end: usize = key_end + 8 + usize::from_le_bytes(value_len_bytes);
        let value_slice = &buf[(key_end + 8)..value_end];
        let value = V::decode(value_slice)?;

        Ok((key, value))
    }
}
