mod admission;
mod flow;
mod model;
mod validation;
mod verify;

pub use admission::admit_embedded_sql;
pub use model::{
    BasicBlock, BlockId, CursorId, CursorStatusAttribute, ExceptionRoute, Instruction, Program,
    ProgramIdentity, SlotDefinition, SlotId, SpannedInstruction, SpannedTerminator,
    SqlStatusAttribute, Terminator,
};
pub use verify::{verify, VerifiedProgram, MAX_STATIC_IR_INSTRUCTIONS, MAX_STATIC_IR_NODES};
