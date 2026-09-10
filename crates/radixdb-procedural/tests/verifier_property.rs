use proptest::prelude::*;
use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_procedural::{
    verify, BasicBlock, BlockId, Instruction, Program, ProgramIdentity, RuntimeType, RuntimeValue,
    SlotDefinition, SlotId, SpannedInstruction, SpannedTerminator, Terminator,
};

fn object(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker.max(1); 16]).unwrap()
}

fn integer_slot(index: usize) -> SlotDefinition {
    SlotDefinition::new(
        format!("slot_{index}"),
        RuntimeType::scalar(CatalogDataType::scalar(DataType::Integer).unwrap(), true),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// This is a bounded structural fuzzer for the public verifier boundary.
    /// Targets and slots deliberately range beyond the generated vectors: the
    /// verifier must either produce a typed error or return a fully admitted
    /// program, never panic or index unchecked input.
    #[test]
    fn arbitrary_cfg_and_slot_references_never_panic(
        marker in any::<u8>(),
        slot_count in 0usize..12,
        entry in 0u32..20,
        blocks in prop::collection::vec(
            (
                prop::collection::vec((0u8..3, 0u32..20, 0u32..20, any::<i64>()), 0..12),
                0u8..4,
                0u32..20,
                0u32..20,
            ),
            0..12,
        ),
    ) {
        let slots = (0..slot_count).map(integer_slot).collect();
        let blocks = blocks
            .into_iter()
            .map(|(instructions, terminator_kind, first, second)| {
                let instructions = instructions
                    .into_iter()
                    .map(|(kind, destination, source, value)| {
                        let instruction = match kind {
                            0 => Instruction::InitializeNull { destination: SlotId(destination) },
                            1 => Instruction::LoadConstant {
                                destination: SlotId(destination),
                                value: RuntimeValue::scalar(Value::Integer(value)),
                            },
                            _ => Instruction::Copy {
                                destination: SlotId(destination),
                                source: SlotId(source),
                            },
                        };
                        SpannedInstruction::unspanned(instruction)
                    })
                    .collect();
                let terminator = match terminator_kind {
                    0 => Terminator::Jump(BlockId(first)),
                    1 => Terminator::Branch {
                        condition: SlotId(first),
                        when_true: BlockId(first),
                        when_false: BlockId(second),
                    },
                    2 => Terminator::Return(Some(SlotId(first))),
                    _ => Terminator::Return(None),
                };
                BasicBlock::new(
                    instructions,
                    SpannedTerminator::unspanned(terminator),
                )
            })
            .collect();
        let candidate = Program::new(
            ProgramIdentity::new(object(marker), 1, "property_cfg"),
            slots,
            Vec::new(),
            None,
            blocks,
            BlockId(entry),
        );

        let _ = verify(candidate);
    }

    #[test]
    fn admitted_straight_line_program_is_stable_under_reverification(
        marker in 1u8..=255,
        values in prop::collection::vec(any::<i64>(), 1..32),
    ) {
        let slots = (0..values.len()).map(integer_slot).collect::<Vec<_>>();
        let instructions = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                SpannedInstruction::unspanned(Instruction::LoadConstant {
                    destination: SlotId(index as u32),
                    value: RuntimeValue::scalar(Value::Integer(value)),
                })
            })
            .collect();
        let candidate = Program::new(
            ProgramIdentity::new(object(marker), 1, "property_valid"),
            slots,
            Vec::new(),
            None,
            vec![BasicBlock::new(
                instructions,
                SpannedTerminator::unspanned(Terminator::Return(None)),
            )],
            BlockId(0),
        );

        let verified = verify(candidate.clone()).expect("generated program must verify");
        prop_assert_eq!(verified.program(), &candidate);
        prop_assert!(verify(candidate).is_ok());
    }
}
