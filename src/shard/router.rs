// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

pub const SLOT_COUNT: u16 = 16384;

pub fn slot_for_key(key: &[u8]) -> u16 {
    let effective = extract_hashtag(key);
    crc16::State::<crc16::XMODEM>::calculate(effective) % SLOT_COUNT
}

fn extract_hashtag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key.iter().skip(start + 1).position(|&b| b == b'}') {
            let inner = &key[start + 1..start + 1 + end];
            if !inner.is_empty() {
                return inner;
            }
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_for_key() {
        let slot = slot_for_key(b"mykey");
        assert!(slot < SLOT_COUNT);
    }

    #[test]
    fn test_hashtag_extraction() {
        assert_eq!(extract_hashtag(b"user:{123}:count"), b"123");
        assert_eq!(extract_hashtag(b"{}empty"), b"{}empty");
        assert_eq!(extract_hashtag(b"no_hashtag"), b"no_hashtag");
    }

    #[test]
    fn test_hashtag_same_slot() {
        let s1 = slot_for_key(b"user:{100}:name");
        let s2 = slot_for_key(b"user:{100}:count");
        assert_eq!(s1, s2);
    }
}
