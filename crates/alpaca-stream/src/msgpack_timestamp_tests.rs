use super::{TimestampExtensionError, decode_timestamp_extension};
use crate::model::ProviderTimestamp;

fn decode_64(seconds: u64, nanosecond: u32) -> Result<ProviderTimestamp, TimestampExtensionError> {
    let packed = (u64::from(nanosecond) << 34) | seconds;
    decode_timestamp_extension(-1, &packed.to_be_bytes())
}

fn decode_96(seconds: i64, nanosecond: u32) -> Result<ProviderTimestamp, TimestampExtensionError> {
    let mut payload = [0; 12];
    payload[..4].copy_from_slice(&nanosecond.to_be_bytes());
    payload[4..].copy_from_slice(&seconds.to_be_bytes());
    decode_timestamp_extension(-1, &payload)
}

#[test]
fn decodes_timestamp_32_unsigned_seconds_boundaries() {
    let zero =
        decode_timestamp_extension(-1, &0_u32.to_be_bytes()).expect("zero timestamp should decode");
    assert_eq!(zero.unix_seconds(), 0);
    assert_eq!(zero.nanosecond(), 0);

    let maximum = decode_timestamp_extension(-1, &u32::MAX.to_be_bytes())
        .expect("timestamp32 maximum should fit chrono");
    assert_eq!(maximum.unix_seconds(), i64::from(u32::MAX));
    assert_eq!(maximum.nanosecond(), 0);
}

#[test]
fn decodes_timestamp_64_seconds_and_nanosecond_boundaries() {
    const MAX_SECONDS_34: u64 = (1_u64 << 34) - 1;

    let zero = decode_64(0, 0).expect("zero timestamp should decode");
    assert_eq!(zero.unix_seconds(), 0);
    assert_eq!(zero.nanosecond(), 0);

    let maximum = decode_64(MAX_SECONDS_34, 999_999_999)
        .expect("maximum timestamp64 fields should fit chrono");
    assert_eq!(
        maximum.unix_seconds(),
        i64::try_from(MAX_SECONDS_34).expect("timestamp64 seconds fit i64")
    );
    assert_eq!(maximum.nanosecond(), 999_999_999);
}

#[test]
fn decodes_timestamp_96_signed_seconds_and_full_nanoseconds() {
    let before_epoch =
        decode_96(-1, 999_999_999).expect("negative seconds and maximum nanoseconds should decode");
    assert_eq!(before_epoch.unix_seconds(), -1);
    assert_eq!(before_epoch.nanosecond(), 999_999_999);
    assert_eq!(before_epoch.as_rfc3339(), "");

    let epoch = decode_96(0, 1).expect("epoch with one nanosecond should decode");
    assert_eq!(epoch.unix_seconds(), 0);
    assert_eq!(epoch.nanosecond(), 1);
}

#[test]
fn rejects_wrong_extension_type_and_payload_lengths() {
    assert_eq!(
        decode_timestamp_extension(0, &0_u32.to_be_bytes()),
        Err(TimestampExtensionError::WrongExtensionType)
    );
    assert_eq!(
        decode_timestamp_extension(-2, &0_u32.to_be_bytes()),
        Err(TimestampExtensionError::WrongExtensionType)
    );

    for length in 0..=13 {
        if [4, 8, 12].contains(&length) {
            continue;
        }
        assert_eq!(
            decode_timestamp_extension(-1, &vec![0; length]),
            Err(TimestampExtensionError::InvalidPayloadLength),
            "unexpected result for payload length {length}"
        );
    }
}

#[test]
fn rejects_nanoseconds_at_or_above_one_billion() {
    assert_eq!(
        decode_64(0, 1_000_000_000),
        Err(TimestampExtensionError::InvalidNanoseconds)
    );
    assert_eq!(
        decode_96(0, 1_000_000_000),
        Err(TimestampExtensionError::InvalidNanoseconds)
    );
}

#[test]
fn rejects_seconds_outside_chrono_range_without_panicking() {
    for seconds in [i64::MIN, i64::MAX] {
        let result = std::panic::catch_unwind(|| decode_96(seconds, 0));
        assert_eq!(
            result.expect("timestamp conversion must not panic"),
            Err(TimestampExtensionError::OutOfChronoRange)
        );
    }
}

#[test]
fn rejects_malformed_32_bit_extension_without_panicking() {
    let result = std::panic::catch_unwind(|| decode_timestamp_extension(-1, &[0; 3]));
    assert_eq!(
        result.expect("malformed extension must not panic"),
        Err(TimestampExtensionError::InvalidPayloadLength)
    );
}
