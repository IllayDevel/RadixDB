//! Safe authoring surface for trusted native RadixDB plugins.

#![forbid(unsafe_op_in_unsafe_fn)]

mod call;
mod codec;
mod error;
pub mod testing;
mod value;

pub use ::radixdb_plugin_macros::{
    radixdb_aggregate, radixdb_batch, radixdb_operator, radixdb_operator_class,
    radixdb_planner_support, radixdb_plugin, radixdb_scalar, radixdb_tvf, radixdb_window,
    RadixType,
};
pub use call::{
    CallContext, CandidatePlanBuilder, CandidateSpan, ColumnBuilder, ColumnView, HashSink,
    PredicateView,
};
pub use codec::{CodecReader, CodecWriter, ManualCodec};
pub use error::{PluginError, PluginErrorKind, PluginResult};
pub use value::{BoundedBytes, BoundedText, RadixType, ValueType};

pub mod prelude {
    pub use crate::{
        radixdb_aggregate, radixdb_batch, radixdb_operator, radixdb_operator_class,
        radixdb_planner_support, radixdb_plugin, radixdb_scalar, radixdb_tvf, radixdb_window,
        BoundedBytes, BoundedText, CallContext, CandidatePlanBuilder, CandidateSpan, CodecReader,
        CodecWriter, ColumnBuilder, ColumnView, HashSink, ManualCodec, PluginError,
        PluginErrorKind, PluginResult, PredicateView, RadixType, ValueType,
    };
}

#[doc(hidden)]
pub mod __private {
    pub use crate::call::{
        column_builder, run_batch, run_codec_decode, run_codec_encode, run_compare, run_equal,
        run_hash, run_key_encoder, run_planner, run_scalar, BatchInput, ResultBuilder,
    };
    pub use crate::codec::{decode_sequence, encode_sequence, CanonicalField};
    pub use radixdb_plugin_abi as abi;
}
