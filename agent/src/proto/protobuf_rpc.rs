/// the message reference <https://github.com/bruceran/krpc/blob/master/misc/protos/krpcmeta.proto>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KrpcMeta {
    #[prost(enumeration="krpc_meta::Direction", tag="1")]
    pub direction: i32,
    #[prost(int32, tag="2")]
    pub service_id: i32,
    #[prost(int32, tag="3")]
    pub msg_id: i32,
    #[prost(int32, tag="4")]
    pub sequence: i32,
    #[prost(int32, tag="6")]
    pub ret_code: i32,
    #[prost(message, optional, tag="7")]
    pub trace: ::core::option::Option<krpc_meta::Trace>,
}
/// Nested message and enum types in `KrpcMeta`.
pub mod krpc_meta {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Trace {
        #[prost(string, tag="2")]
        pub trace_id: ::prost::alloc::string::String,
        #[prost(string, tag="4")]
        pub span_id: ::prost::alloc::string::String,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Direction {
        InvalidDirection = 0,
        Request = 1,
        Response = 2,
    }
    impl Direction {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Direction::InvalidDirection => "INVALID_DIRECTION",
                Direction::Request => "REQUEST",
                Direction::Response => "RESPONSE",
            }
        }
    }
}
