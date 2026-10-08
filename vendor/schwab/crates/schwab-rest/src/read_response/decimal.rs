//! Exact bounded decimal and ratio values for numeric response fields.
//! 为数值响应字段提供精确且有界的十进制数与比值。

use std::fmt;

use serde_json::Number;

use super::ReadResponseError;

const MAX_DECIMAL_BYTES: usize = 64;
const MAX_DECIMAL_SCALE: u32 = 18;

/// Arbitrary-precision base-10 response number represented as a signed
/// coefficient and scale. It avoids binary floating point in quote prices.
/// 中文摘要：保留十进制精度且受 i128 系数和 scale 上限约束的数值。
#[derive(Clone, Eq, PartialEq)]
pub struct ExactDecimal {
    pub(super) coefficient: i128,
    pub(super) scale: u32,
}

impl ExactDecimal {
    /// Performs parse for exact decimal.
    /// 执行 exact decimal 的 parse 操作。
    pub fn parse(value: &str) -> Result<Self, ReadResponseError> {
        if value.is_empty() || value.len() > MAX_DECIMAL_BYTES {
            return Err(ReadResponseError::DecimalOutOfRange);
        }

        let (negative, unsigned) = match value.as_bytes().first() {
            Some(b'-') => (true, &value[1..]),
            Some(b'+') => (false, &value[1..]),
            _ => (false, value),
        };
        let exponent_at = unsigned.find(['e', 'E']);
        let (mantissa, exponent) = match exponent_at {
            Some(index) => {
                let exponent = unsigned[index + 1..]
                    .parse::<i32>()
                    .map_err(|_| ReadResponseError::DecimalOutOfRange)?;
                (&unsigned[..index], exponent)
            }
            None => (unsigned, 0),
        };
        if mantissa.is_empty() {
            return Err(ReadResponseError::DecimalOutOfRange);
        }

        let mut digits = [0_u8; MAX_DECIMAL_BYTES];
        let mut digits_len = 0usize;
        let mut fractional_digits = 0i32;
        let mut integer_digits = 0usize;
        let mut decimal_seen = false;
        for byte in mantissa.bytes() {
            match byte {
                b'0'..=b'9' => {
                    digits[digits_len] = byte;
                    digits_len += 1;
                    if decimal_seen {
                        fractional_digits += 1;
                    } else {
                        integer_digits += 1;
                    }
                }
                b'.' if !decimal_seen => decimal_seen = true,
                _ => return Err(ReadResponseError::DecimalOutOfRange),
            }
        }
        if digits_len == 0 || integer_digits == 0 || (decimal_seen && fractional_digits == 0) {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        let mut scale = fractional_digits
            .checked_sub(exponent)
            .ok_or(ReadResponseError::DecimalOutOfRange)?;
        if scale < 0 {
            let zeros =
                usize::try_from(-scale).map_err(|_| ReadResponseError::DecimalOutOfRange)?;
            if digits_len.saturating_add(zeros) > MAX_DECIMAL_BYTES {
                return Err(ReadResponseError::DecimalOutOfRange);
            }
            digits[digits_len..digits_len + zeros].fill(b'0');
            digits_len += zeros;
            scale = 0;
        }
        let mut scale = u32::try_from(scale).map_err(|_| ReadResponseError::DecimalOutOfRange)?;
        if scale > MAX_DECIMAL_SCALE {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        while scale > 0 && digits_len > 0 && digits[digits_len - 1] == b'0' {
            digits_len -= 1;
            scale -= 1;
        }
        let magnitude = std::str::from_utf8(&digits[..digits_len])
            .map_err(|_| ReadResponseError::DecimalOutOfRange)?
            .parse::<i128>()
            .map_err(|_| ReadResponseError::DecimalOutOfRange)?;
        let coefficient = if negative {
            magnitude
                .checked_neg()
                .ok_or(ReadResponseError::DecimalOutOfRange)?
        } else {
            magnitude
        };
        Ok(Self { coefficient, scale })
    }

    /// Performs as string for exact decimal.
    /// 执行 exact decimal 的 as string 操作。
    pub fn as_string(&self) -> String {
        let negative = self.coefficient < 0;
        let mut digits = self.coefficient.unsigned_abs().to_string();
        if self.scale > 0 {
            let scale = self.scale as usize;
            if digits.len() <= scale {
                let zeros = scale + 1 - digits.len();
                digits.insert_str(0, &"0".repeat(zeros));
            }
            let split = digits.len() - scale;
            digits.insert(split, '.');
        }
        if negative {
            digits.insert(0, '-');
        }
        digits
    }

    pub(super) fn from_integer(value: i128) -> Self {
        Self {
            coefficient: value,
            scale: 0,
        }
    }

    pub(super) fn checked_add(&self, other: &Self) -> Result<Self, ReadResponseError> {
        let (left, right, scale) = self.aligned_coefficients(other)?;
        let coefficient = left
            .checked_add(right)
            .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?;
        Ok(Self { coefficient, scale }.normalized())
    }

    pub(super) fn checked_sub(&self, other: &Self) -> Result<Self, ReadResponseError> {
        let (left, right, scale) = self.aligned_coefficients(other)?;
        let coefficient = left
            .checked_sub(right)
            .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?;
        Ok(Self { coefficient, scale }.normalized())
    }

    pub(super) fn checked_div_two(&self) -> Result<Self, ReadResponseError> {
        if self.coefficient % 2 == 0 {
            return Ok(Self {
                coefficient: self.coefficient / 2,
                scale: self.scale,
            }
            .normalized());
        }
        if self.scale == MAX_DECIMAL_SCALE {
            return Err(ReadResponseError::DecimalArithmeticOutOfRange);
        }
        Ok(Self {
            coefficient: self
                .coefficient
                .checked_mul(5)
                .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?,
            scale: self.scale + 1,
        }
        .normalized())
    }

    pub(super) fn checked_mul_hundred(&self) -> Result<Self, ReadResponseError> {
        Ok(Self {
            coefficient: self
                .coefficient
                .checked_mul(100)
                .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?,
            scale: self.scale,
        }
        .normalized())
    }

    pub(super) fn checked_abs(&self) -> Result<Self, ReadResponseError> {
        Ok(Self {
            coefficient: self
                .coefficient
                .checked_abs()
                .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?,
            scale: self.scale,
        })
    }

    fn aligned_coefficients(&self, other: &Self) -> Result<(i128, i128, u32), ReadResponseError> {
        let scale = self.scale.max(other.scale);
        let left_factor = power_of_ten(scale - self.scale)?;
        let right_factor = power_of_ten(scale - other.scale)?;
        let left = self
            .coefficient
            .checked_mul(left_factor)
            .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?;
        let right = other
            .coefficient
            .checked_mul(right_factor)
            .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?;
        Ok((left, right, scale))
    }

    pub(super) fn normalized(mut self) -> Self {
        while self.scale > 0 && self.coefficient % 10 == 0 {
            self.coefficient /= 10;
            self.scale -= 1;
        }
        self
    }

    pub(super) fn from_number(number: &Number) -> Result<Self, ReadResponseError> {
        if !number.as_f64().is_some_and(f64::is_finite) {
            return Err(ReadResponseError::DecimalOutOfRange);
        }
        Self::parse(&number.to_string())
    }

    pub(super) fn positive(value: Option<Self>) -> Option<Self> {
        value.filter(|decimal| decimal.coefficient > 0)
    }
}

impl fmt::Debug for ExactDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactDecimal([REDACTED])")
    }
}

impl fmt::Display for ExactDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.as_string())
    }
}

/// Exact percent ratio `(numerator / denominator) * 100`. It intentionally
/// avoids a rounded IEEE-754 percentage.
/// 中文摘要：由两个精确十进制值构成的未舍入比值。
#[derive(Clone, Eq, PartialEq)]
pub struct ExactRatio {
    pub(super) numerator: ExactDecimal,
    pub(super) denominator: ExactDecimal,
}

impl ExactRatio {
    /// Performs numerator for exact ratio.
    /// 执行 exact ratio 的 numerator 操作。
    pub fn numerator(&self) -> &ExactDecimal {
        &self.numerator
    }

    /// Performs denominator for exact ratio.
    /// 执行 exact ratio 的 denominator 操作。
    pub fn denominator(&self) -> &ExactDecimal {
        &self.denominator
    }
}

impl fmt::Debug for ExactRatio {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExactRatio([REDACTED])")
    }
}

fn power_of_ten(power: u32) -> Result<i128, ReadResponseError> {
    let mut result = 1i128;
    for _ in 0..power {
        result = result
            .checked_mul(10)
            .ok_or(ReadResponseError::DecimalArithmeticOutOfRange)?;
    }
    Ok(result)
}
