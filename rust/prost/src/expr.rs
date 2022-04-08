#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ExprNode {
    #[prost(enumeration = "expr_node::Type", tag = "1")]
    pub expr_type: i32,
    #[prost(message, optional, tag = "3")]
    pub return_type: ::core::option::Option<super::data::DataType>,
    #[prost(oneof = "expr_node::RexNode", tags = "4, 5, 6")]
    pub rex_node: ::core::option::Option<expr_node::RexNode>,
}
/// Nested message and enum types in `ExprNode`.
pub mod expr_node {
    #[derive(
        prost_helpers::AnyPB,
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration,
    )]
    #[repr(i32)]
    pub enum Type {
        Invalid = 0,
        InputRef = 1,
        ConstantValue = 2,
        /// arithmetics operators
        Add = 3,
        Subtract = 4,
        Multiply = 5,
        Divide = 6,
        Modulus = 7,
        /// comparison operators
        Equal = 8,
        NotEqual = 9,
        LessThan = 10,
        LessThanOrEqual = 11,
        GreaterThan = 12,
        GreaterThanOrEqual = 13,
        /// logical operators
        And = 21,
        Or = 22,
        Not = 23,
        In = 24,
        /// date functions
        Extract = 101,
        PgSleep = 102,
        TumbleStart = 103,
        /// other functions
        Cast = 201,
        Substr = 202,
        Length = 203,
        Like = 204,
        Upper = 205,
        Lower = 206,
        Trim = 207,
        Replace = 208,
        Position = 209,
        Ltrim = 210,
        Rtrim = 211,
        Case = 212,
        /// ROUND(numeric, integer) -> numeric
        RoundDigit = 213,
        /// ROUND(numeric) -> numeric
        /// ROUND(double precision) -> double precision
        Round = 214,
        Ascii = 215,
        Translate = 216,
        /// Boolean comparison
        IsTrue = 301,
        IsNotTrue = 302,
        IsFalse = 303,
        IsNotFalse = 304,
        IsNull = 305,
        IsNotNull = 306,
        /// Unary operators
        Neg = 401,
        /// Search operator and Search ARGument
        Search = 998,
        Sarg = 999,
        StreamNullByRowCount = 1000,
    }
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum RexNode {
        #[prost(message, tag = "4")]
        InputRef(super::InputRefExpr),
        #[prost(message, tag = "5")]
        Constant(super::ConstantValue),
        #[prost(message, tag = "6")]
        FuncCall(super::FunctionCall),
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct InputRefExpr {
    #[prost(int32, tag = "1")]
    pub column_idx: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ConstantValue {
    /// bool array/bitmap: one byte, 0 for false (null), non-zero for true (non-null)
    /// integer, float,  double: big-endianness
    /// char, varchar: encoded accorded to encoding, currently only utf8 is supported.
    #[prost(bytes = "vec", tag = "1")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct FunctionCall {
    #[prost(message, repeated, tag = "1")]
    pub children: ::prost::alloc::vec::Vec<ExprNode>,
}
/// Aggregate Function Calls for Aggregation
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AggCall {
    #[prost(enumeration = "agg_call::Type", tag = "1")]
    pub r#type: i32,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<agg_call::Arg>,
    #[prost(message, optional, tag = "3")]
    pub return_type: ::core::option::Option<super::data::DataType>,
    #[prost(bool, tag = "4")]
    pub distinct: bool,
}
/// Nested message and enum types in `AggCall`.
pub mod agg_call {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct Arg {
        #[prost(message, optional, tag = "1")]
        pub input: ::core::option::Option<super::InputRefExpr>,
        #[prost(message, optional, tag = "2")]
        pub r#type: ::core::option::Option<super::super::data::DataType>,
    }
    #[derive(
        prost_helpers::AnyPB,
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration,
    )]
    #[repr(i32)]
    pub enum Type {
        Invalid = 0,
        Sum = 1,
        Min = 2,
        Max = 3,
        Count = 4,
        Avg = 5,
        StringAgg = 6,
        SingleValue = 7,
    }
}
