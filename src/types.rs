use crate::Result;
use arrow::datatypes::{DataType, Field};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Type {
    Int,
    Decimal(i8),
    Float,
    Bool,
    Text,
    Binary,
    Date,
    Null,
}
impl Type {
    // Internal Arrow fields preserve SQL INTEGER identity while carrying exact
    // precision-38 values. DECIMAL(38,0) without this marker stays DECIMAL.
    const WIDE_INTEGER: &'static str = "assay.internal.integer.v1";

    pub(crate) fn from_field(field: &Field) -> Result<Self> {
        if Self::is_wide_integer(field) {
            Ok(Self::Int)
        } else {
            Self::from_arrow(field.data_type())
        }
    }

    pub(crate) fn is_wide_integer(field: &Field) -> bool {
        field.data_type() == &DataType::Decimal128(38, 0)
            && field
                .metadata()
                .get(Self::WIDE_INTEGER)
                .is_some_and(|v| v == "true")
    }

    pub(crate) fn field(&self, name: &str, internal: bool) -> Field {
        if internal && *self == Self::Int {
            Field::new(name, DataType::Decimal128(38, 0), true)
                .with_metadata([(Self::WIDE_INTEGER.to_string(), "true".to_string())].into())
        } else {
            Field::new(name, self.arrow(), true)
        }
    }
    pub(crate) fn has_internal_metadata(schema: &arrow::datatypes::Schema) -> bool {
        schema
            .fields()
            .iter()
            .any(|f| f.metadata().contains_key(Self::WIDE_INTEGER))
    }

    /// External files cannot assign engine-internal SQL type identity.
    pub(crate) fn external_schema(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema {
        let fields = schema
            .fields()
            .iter()
            .map(|f| {
                let mut metadata = f.metadata().clone();
                metadata.remove(Self::WIDE_INTEGER);
                f.as_ref().clone().with_metadata(metadata)
            })
            .collect::<Vec<_>>();
        arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone())
    }

    pub fn from_arrow(t: &DataType) -> Result<Self> {
        Ok(match t {
            DataType::Int32 | DataType::Int64 => Self::Int,
            DataType::Decimal128(_, s) if (0..=38).contains(s) => Self::Decimal(*s),
            DataType::Utf8 => Self::Text,
            DataType::Binary => Self::Binary,
            DataType::Float64 => Self::Float,
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Bool,
            DataType::Date32 => Self::Date,
            _ => return Err(format!("unsupported input type: {t}")),
        })
    }
    pub fn arrow(&self) -> DataType {
        match self {
            Self::Int => DataType::Int64,
            Self::Decimal(s) => DataType::Decimal128(38, *s),
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Text => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::Null => DataType::Null,
        }
    }
    pub fn scale(&self) -> Result<i8> {
        match self {
            Self::Int => Ok(0),
            Self::Decimal(s) => Ok(*s),
            _ => Err(format!("expected numeric type, got {self:?}")),
        }
    }
}
/// SQL hexadecimal bytes, without text decoding or numeric interpretation.
pub fn binary_literal(text: &str) -> Result<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return Err(
            "semantic: binary literal requires an even number of hexadecimal digits".into(),
        );
    }
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = digit(pair[0]).ok_or("semantic: invalid hexadecimal binary literal")?;
            let low = digit(pair[1]).ok_or("semantic: invalid hexadecimal binary literal")?;
            Ok((high << 4) | low)
        })
        .collect()
}
/// Lossless wire representation. Its schema tag is `binary`, never `str`.
pub fn binary_hex(bytes: &[u8]) -> Result<String> {
    let capacity = bytes
        .len()
        .checked_mul(2)
        .ok_or("binary output size overflow")?;
    let mut result = String::with_capacity(capacity);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 15) as usize] as char);
    }
    Ok(result)
}
pub fn factor(scale: i8) -> Result<i128> {
    if !(0..=38).contains(&scale) {
        return Err("decimal scale outside 0..38".into());
    }
    10_i128
        .checked_pow(scale as u32)
        .ok_or_else(|| "decimal scale overflow".into())
}
#[inline]
pub fn checked(value: Option<i128>) -> Result<i128> {
    let n = value.ok_or("numeric overflow")?;
    const LIMIT: i128 = 10_i128.pow(38);
    if n <= -LIMIT || n >= LIMIT {
        return Err("decimal precision 38 overflow".into());
    }
    Ok(n)
}
pub fn rescale(n: i128, old: i8, new: i8) -> Result<i128> {
    if new < old {
        return Err("implicit decimal scale loss is unsupported".into());
    }
    checked(n.checked_mul(factor(new - old)?))
}
pub fn decimal_literal(s: &str) -> Result<(i128, i8)> {
    let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
    if fraction.len() > 18
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("unsupported numeric literal: {s}"));
    }
    let digits = format!("{whole}{fraction}");
    Ok((checked(digits.parse::<i128>().ok())?, fraction.len() as i8))
}
pub fn decimal_text(n: i128, scale: i8) -> String {
    if scale == 0 {
        return n.to_string();
    }
    let digits = format!("{:0width$}", n.unsigned_abs(), width = scale as usize + 1);
    let split = digits.len() - scale as usize;
    format!(
        "{}{}.{}",
        if n < 0 { "-" } else { "" },
        &digits[..split],
        &digits[split..]
    )
}
