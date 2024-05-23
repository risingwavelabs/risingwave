use core::cell::RefCell;
use std::rc::Rc;

use winnow::combinator::{
    alt, delimited, dispatch, empty, fail, opt, preceded, repeat, separated, seq, Repeat,
};
use winnow::error::StrContext;
use winnow::{PResult, Parser, Stateful};

use super::{
    identifier_non_reserved, keyword, literal_uint, precision_in_range, token, with_state,
    TokenStream,
};
use crate::ast::{DataType, Ident, ObjectName, StructField};
use crate::keywords::Keyword;
use crate::tokenizer::Token;

#[derive(Default, Debug)]
struct DataTypeParsingState {
    /// Since we can't distinguish between `>>` and `> >` in tokenizer, we need to handle this case in the parser.
    /// When we want a [`>`][Token::Gt] but actually consumed a [`>>`][Token::ShiftRight], we set this to true.
    /// When the value was true and we want a [`>`][Token::Gt], we just set this to false instead of really consume it.
    remaining_close: Rc<RefCell<bool>>,
}

type StatefulStream<S> = Stateful<S, DataTypeParsingState>;

fn struct_data_type<S>(input: &mut StatefulStream<S>) -> PResult<Vec<StructField>>
where
    S: TokenStream,
{
    let remaining_close1 = input.state.remaining_close.clone();
    let remaining_close2 = input.state.remaining_close.clone();

    let consume_close = alt((
        move |input: &mut StatefulStream<S>| -> PResult<()> {
            if *remaining_close1.borrow() {
                Ok(())
            } else {
                fail(input)
            }
        }
        .void(),
        (
            Token::ShiftRight,
            move |_input: &mut StatefulStream<S>| -> PResult<()> {
                *remaining_close2.borrow_mut() = true;
                Ok(())
            },
        )
            .void(),
        Token::Gt.void(),
    ));

    delimited(
        Token::Lt,
        separated(
            1..,
            seq! {
                StructField {
                    name: identifier_non_reserved,
                    _: Token::Colon,
                    data_type: data_type_stateful,
                }
            },
            Token::Comma,
        ),
        consume_close,
    )
    .parse_next(input)
}

pub fn data_type<S>(input: &mut S) -> PResult<DataType>
where
    S: TokenStream,
{
    with_state::<S, DataTypeParsingState, _, _>(data_type_stateful)
        .context(StrContext::Label("data_type"))
        .parse_next(input)
}

fn data_type_stateful<S>(input: &mut StatefulStream<S>) -> PResult<DataType>
where
    S: TokenStream,
{
    repeat(0.., (Token::LBracket, Token::RBracket))
        .fold1(data_type_stateful_inner, |mut acc, _| {
            acc = DataType::Array(Box::new(acc));
            acc
        })
        .parse_next(input)
}

fn data_type_stateful_inner<S>(input: &mut StatefulStream<S>) -> PResult<DataType>
where
    S: TokenStream,
{
    let with_time_zone = || {
        opt(alt((
            (Keyword::WITH, Keyword::TIME, Keyword::ZONE).value(true),
            (Keyword::WITHOUT, Keyword::TIME, Keyword::ZONE).value(false),
        )))
        .map(|x| x.unwrap_or(false))
    };

    let precision_and_scale = || {
        opt(delimited(
            Token::LParen,
            (literal_uint, opt(preceded(Token::Comma, literal_uint))),
            Token::RParen,
        ))
        .map(|p| match p {
            Some((x, y)) => (Some(x), y),
            None => (None, None),
        })
    };

    let keywords = dispatch! {keyword;
        Keyword::BOOLEAN | Keyword::BOOL => empty.value(DataType::Boolean),
        Keyword::FLOAT => opt(precision_in_range(1..53)).map(DataType::Float),
        Keyword::REAL => empty.value(DataType::Real),
        Keyword::DOUBLE => Keyword::PRECISION.value(DataType::Double),
        Keyword::SMALLINT => empty.value(DataType::SmallInt),
        Keyword::INT | Keyword::INTEGER => empty.value(DataType::Int),
        Keyword::BIGINT => empty.value(DataType::BigInt),
        Keyword::STRING | Keyword::VARCHAR => empty.value(DataType::Varchar),
        Keyword::CHAR | Keyword::CHARACTER => dispatch! {keyword;
            Keyword::VARYING => empty.value(DataType::Varchar),
            _ => opt(precision_in_range(..)).map(DataType::Char),
        },
        Keyword::UUID => empty.value(DataType::Uuid),
        Keyword::DATE => empty.value(DataType::Date),
        Keyword::TIMESTAMP => with_time_zone().map(DataType::Timestamp),
        Keyword::TIME => with_time_zone().map(DataType::Time),
        // TODO: Support complex interval type parsing.
        Keyword::INTERVAL => empty.value(DataType::Interval),
        Keyword::REGCLASS => empty.value(DataType::Regclass),
        Keyword::REGPROC => empty.value(DataType::Regproc),
        Keyword::STRUCT => struct_data_type.map(DataType::Struct),
        Keyword::BYTEA => empty.value(DataType::Bytea),
        Keyword::NUMERIC | Keyword::DECIMAL | Keyword::DEC => precision_and_scale().map(|(precision, scale)| {
            DataType::Decimal(precision, scale)
        }),
        _ => fail,
    };

    alt((
        keywords,
        token.verify_map(|t| match t.token {
            // JSONB is not a keyword, but a special data type.
            Token::Word(w) if w.value.eq_ignore_ascii_case("jsonb") => Some(DataType::Jsonb),
            // FIXME: Really parse a full object name here.
            Token::Word(w) => Some(DataType::Custom(ObjectName::from(vec![
                Ident::new_unchecked(w.value),
            ]))),
            _ => None,
        }),
    ))
    .context(StrContext::Label("data_type_inner"))
    .parse_next(input)
}
