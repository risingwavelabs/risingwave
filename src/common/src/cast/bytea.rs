use itertools::Itertools;
use snafu::{OptionExt, Snafu};

type Result<T> = std::result::Result<T, ByteaCastError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum ByteaCastError {
    #[snafu(display("invalid hex digit `{from}`"))]
    InvalidHexDigit { from: char },

    #[snafu(display("invalid base64 symbol: `{from}`"))]
    InvalidBase64 { from: char },

    #[snafu(display("invalid format (`{from}`) for bytea: {message}"))]
    InvalidBytea { from: Box<str>, message: Box<str> },
}

impl From<ByteaCastError> for crate::error::RwError {
    fn from(value: ByteaCastError) -> Self {
        crate::error::ErrorCode::ExprError(value.into()).into()
    }
}

/// Refer to PostgreSQL's implementation <https://github.com/postgres/postgres/blob/5cb54fc310fb84287cbdc74533f3420490a2f63a/src/backend/utils/adt/varlena.c#L276-L288>
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>> {
    if let Some(remainder) = elem.strip_prefix(r"\x") {
        Ok(parse_bytes_hex(remainder)?.into())
    } else {
        Ok(parse_bytes_traditional(elem)?.into())
    }
}

/// Ref: <https://docs.rs/hex/0.4.3/src/hex/lib.rs.html#175-185>
#[inline(always)]
fn get_hex(c: u8) -> Result<u8> {
    match c {
        b'A'..=b'F' => Ok(c - b'A' + 10),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'0'..=b'9' => Ok(c - b'0'),
        _ => InvalidHexDigitSnafu { from: c as char }.fail(),
    }
}

/// Refer to <https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10> for specification.
pub fn parse_bytes_hex(s: &str) -> Result<Vec<u8>> {
    let mut res = Vec::with_capacity(s.len() / 2);

    let mut bytes = s.bytes();
    while let Some(c) = bytes.next() {
        // white spaces are tolerated
        if c == b' ' || c == b'\n' || c == b'\t' || c == b'\r' {
            continue;
        }
        let v1 = get_hex(c)?;

        let c = bytes.next().context(InvalidByteaSnafu {
            from: s,
            message: "odd number of digits in hex format",
        })?;

        let v2 = get_hex(c)?;
        res.push((v1 << 4) | v2);
    }

    Ok(res)
}

/// Refer to <https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10> for specification.
pub fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>> {
    let context = InvalidByteaSnafu {
        from: s,
        message: "invalid input syntax for escape format",
    };

    let mut bytes = s.bytes();

    let mut res = Vec::new();
    while let Some(b) = bytes.next() {
        if b != b'\\' {
            res.push(b);
        } else {
            match bytes.next() {
                Some(b'\\') => {
                    res.push(b'\\');
                }
                Some(b1 @ b'0'..=b'3') => match bytes.next_tuple() {
                    Some((b2 @ b'0'..=b'7', b3 @ b'0'..=b'7')) => {
                        res.push(((b1 - b'0') << 6) + ((b2 - b'0') << 3) + (b3 - b'0'));
                    }
                    _ => {
                        // one backslash, not followed by another or ### valid octal
                        return context.fail();
                    }
                },
                _ => {
                    // one backslash, not followed by another or ### valid octal
                    return context.fail();
                }
            }
        }
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytea() {
        use crate::types::ToText;
        assert_eq!(str_to_bytea("fgo").unwrap().as_ref().to_text(), r"\x66676f");
        assert_eq!(
            str_to_bytea(r"\xDeadBeef").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("12CD").unwrap().as_ref().to_text(),
            r"\x31324344"
        );
        assert_eq!(
            str_to_bytea("1234").unwrap().as_ref().to_text(),
            r"\x31323334"
        );
        assert_eq!(
            str_to_bytea(r"\x12CD").unwrap().as_ref().to_text(),
            r"\x12cd"
        );
        assert_eq!(
            str_to_bytea(r"\x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\x7820446520416420426520456620"
        );
        assert_eq!(
            str_to_bytea(r"De\\123dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3132336442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\123dBeEf").unwrap().as_ref().to_text(),
            r"\x4465536442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\\000dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3030306442654566"
        );

        assert_eq!(str_to_bytea(r"\123").unwrap().as_ref().to_text(), r"\x53");
        assert_eq!(str_to_bytea(r"\\").unwrap().as_ref().to_text(), r"\x5c");
        assert_eq!(
            str_to_bytea(r"123").unwrap().as_ref().to_text(),
            r"\x313233"
        );
        assert_eq!(
            str_to_bytea(r"\\123").unwrap().as_ref().to_text(),
            r"\x5c313233"
        );

        assert!(str_to_bytea(r"\1").is_err());
        assert!(str_to_bytea(r"\12").is_err());
        assert!(str_to_bytea(r"\400").is_err());
        assert!(str_to_bytea(r"\378").is_err());
        assert!(str_to_bytea(r"\387").is_err());
        assert!(str_to_bytea(r"\377").is_ok());
    }
}
