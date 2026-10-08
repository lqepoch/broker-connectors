//! Strict decoding for the `MessagePack` Timestamp extension.
//!
//! `MessagePack` Timestamp 扩展的严格解码。

use crate::model::ProviderTimestamp;

const TIMESTAMP_EXTENSION_TYPE: i8 = -1;
const MAX_NANOSECONDS: u32 = 1_000_000_000;
const TIMESTAMP64_SECONDS_MASK: u64 = (1_u64 << 34) - 1;

/// Fixed error categories for malformed or unrepresentable `MessagePack` timestamps.
/// 对格式错误或无法表示的 `MessagePack` 时间戳使用固定错误类别。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimestampExtensionError {
    /// The extension type is not the predefined Timestamp type.
    /// 扩展类型不是预定义的 Timestamp 类型。
    WrongExtensionType,
    /// The timestamp payload is not 4, 8, or 12 bytes long.
    /// 时间戳载荷长度不是 4、8 或 12 字节。
    InvalidPayloadLength,
    /// The decoded nanosecond component is outside the valid range.
    /// 解出的纳秒分量超出有效范围。
    InvalidNanoseconds,
    /// Chrono cannot represent the decoded second and nanosecond components.
    /// Chrono 无法表示解出的秒和纳秒分量。
    OutOfChronoRange,
}

/// Decodes a `MessagePack` Timestamp extension without converting through text or floating point.
/// 解码 `MessagePack` Timestamp 扩展时不经过文本或浮点数转换。
pub(crate) fn decode_timestamp_extension(
    type_code: i8,
    payload: &[u8],
) -> Result<ProviderTimestamp, TimestampExtensionError> {
    if type_code != TIMESTAMP_EXTENSION_TYPE {
        return Err(TimestampExtensionError::WrongExtensionType);
    }

    let (seconds, nanosecond) = match payload.len() {
        4 => {
            let seconds = u32::from_be_bytes(
                payload
                    .try_into()
                    .map_err(|_| TimestampExtensionError::InvalidPayloadLength)?,
            );
            (i64::from(seconds), 0)
        }
        8 => {
            let packed = u64::from_be_bytes(
                payload
                    .try_into()
                    .map_err(|_| TimestampExtensionError::InvalidPayloadLength)?,
            );
            let nanosecond = u32::try_from(packed >> 34)
                .expect("the MessagePack timestamp nanosecond field is at most 30 bits");
            let seconds = i64::try_from(packed & TIMESTAMP64_SECONDS_MASK)
                .expect("the MessagePack timestamp seconds field is at most 34 bits");
            (seconds, nanosecond)
        }
        12 => {
            let nanosecond = u32::from_be_bytes(
                payload
                    .get(..4)
                    .ok_or(TimestampExtensionError::InvalidPayloadLength)?
                    .try_into()
                    .map_err(|_| TimestampExtensionError::InvalidPayloadLength)?,
            );
            let seconds = i64::from_be_bytes(
                payload
                    .get(4..)
                    .ok_or(TimestampExtensionError::InvalidPayloadLength)?
                    .try_into()
                    .map_err(|_| TimestampExtensionError::InvalidPayloadLength)?,
            );
            (seconds, nanosecond)
        }
        _ => return Err(TimestampExtensionError::InvalidPayloadLength),
    };

    if nanosecond >= MAX_NANOSECONDS {
        return Err(TimestampExtensionError::InvalidNanoseconds);
    }

    ProviderTimestamp::from_unix_parts(seconds, nanosecond)
        .ok_or(TimestampExtensionError::OutOfChronoRange)
}

#[cfg(test)]
#[path = "msgpack_timestamp_tests.rs"]
mod tests;
