use super::*;

#[test]
fn complete_package_wires_external_types_and_index_support() {
    let args = quote!(
        id = "7c471f38-2224-431f-9248-c7443267411a",
        name = "indexed",
        version = "1.0.0"
    );
    let module: ItemMod = syn::parse_quote! {
        mod indexed {
            #[radix_type(id = "number", name = "number", codec = 1)]
            struct Number { value: i64 }
            #[radixdb_scalar(id = "equal", name = "equal", semantic_revision = 1,
                immutable, strict, parallel_safe, cost = 1, cancellation = "bounded", max_output_bytes = 1)]
            fn equal(left: Number, right: Number) -> PluginResult<bool> { Ok(left.value == right.value) }
            #[radixdb_batch(for_scalar = "equal", rows_per_cancel_check = 64)]
            fn equal_batch(left: ColumnView<'_, Number>, right: ColumnView<'_, Number>, output: &mut Builder, context: &Context) {}
            #[radixdb_operator(id = "equal_op", symbol = "=", semantic_revision = 1,
                function = "equal", left = Number, right = Number, result = bool)]
            fn equal_operator() {}
            #[radixdb_operator_class(id = "number_hash", semantic_revision = 1,
                access_method = "hash", input = Number, key = i64, key_codec_revision = 1)]
            fn key(value: Number) -> PluginResult<i64> { Ok(value.value) }
            #[radixdb_planner_support(id = "support", name = "support", semantic_revision = 1,
                for_function = "equal", operator_class = "number_hash", max_spans = 4,
                max_output_bytes = 64, always_recheck)]
            fn support(predicate: &[u8], output: &mut Builder) {}
        }
    };
    let output = expand_plugin(parse_options(args.clone()).unwrap(), module.clone()).unwrap();
    syn::parse2::<syn::File>(output.clone()).unwrap();
    assert_eq!(
        output.to_string(),
        expand_plugin(parse_options(args.clone()).unwrap(), module.clone())
            .unwrap()
            .to_string()
    );
    for marker in [
        "RadixAbiExternalTypeDescriptorV1",
        "RadixAbiOperatorDescriptorV1",
        "RadixAbiOperatorClassDescriptorV1",
        "RadixAbiPlannerSupportDescriptorV1",
        "__radixdb_batch_equal_batch",
    ] {
        assert!(output.to_string().contains(marker), "missing {marker}");
    }
    let mut missing_scalar = module;
    missing_scalar
        .content
        .as_mut()
        .unwrap()
        .1
        .retain(|item| !matches!(item, Item::Fn(function) if function.sig.ident == "equal"));
    let error = expand_plugin(parse_options(args).unwrap(), missing_scalar).unwrap_err();
    assert!(error.to_string().contains("unknown scalar"));
}

#[test]
fn operator_classes_require_all_core_strategies() {
    let function: ItemFn = syn::parse_quote!(
        fn key(value: i64) -> PluginResult<i64> {
            Ok(value)
        }
    );
    let types = BTreeMap::new();
    let operators: BTreeMap<_, _> = ["<", "<=", "=", ">=", ">"]
        .into_iter()
        .enumerate()
        .map(|(index, symbol)| {
            (
                (symbol.to_owned(), "i64".to_owned(), "i64".to_owned()),
                [index as u8; 16],
            )
        })
        .collect();
    for (method, constant, count) in [
        ("btree", "BTREE", 5),
        ("hash", "HASH", 1),
        ("bitmap", "BITMAP", 1),
    ] {
        let options = parse_options(quote!(id = "key", semantic_revision = 1,
            access_method = #method, input = i64, key = i64, key_codec_revision = 1))
        .unwrap();
        let generated =
            generate_opclass_descriptor(&function, "key", &options, [1; 16], &types, &operators)
                .unwrap();
        syn::parse2::<syn::File>(generated.items.clone()).unwrap();
        syn::parse2::<syn::Expr>(generated.descriptor.clone()).unwrap();
        assert!(generated
            .descriptor
            .to_string()
            .contains(&format!("RADIX_ACCESS_METHOD_{constant}")));
        assert_eq!(
            generated
                .items
                .to_string()
                .matches("RadixAbiBindingEntryV1 {")
                .count(),
            count
        );
        assert!(generate_opclass_descriptor(
            &function,
            "key",
            &options,
            [1; 16],
            &types,
            &BTreeMap::new()
        )
        .is_err());
    }
    for method in ["hnsw", "custom"] {
        let options = parse_options(
            quote!(semantic_revision = 1, key_codec_revision = 1, access_method = #method),
        )
        .unwrap();
        assert!(generate_opclass_descriptor(
            &function, "key", &options, [1; 16], &types, &operators
        )
        .is_err());
    }
}

#[test]
fn operator_and_planner_bind_only_known_objects() {
    let function: ItemFn = syn::parse_quote!(
        fn support() {}
    );
    let functions = BTreeMap::from([("equal".to_owned(), [3; 16])]);
    let classes = BTreeMap::from([("key".to_owned(), [4; 16])]);
    let options = parse_options(quote!(
        symbol = "=",
        semantic_revision = 1,
        function = "equal",
        left = i64,
        right = i64,
        result = bool
    ))
    .unwrap();
    let descriptor = generate_operator_descriptor(
        &function,
        "equal_op",
        &options,
        [1; 16],
        &BTreeMap::new(),
        &functions,
    )
    .unwrap();
    syn::parse2::<syn::Expr>(descriptor).unwrap();
    assert!(generate_operator_descriptor(
        &function,
        "equal_op",
        &options,
        [1; 16],
        &BTreeMap::new(),
        &BTreeMap::new()
    )
    .is_err());
    for (policy, constant) in [
        (quote!(exact), "RADIX_RECHECK_EXACT"),
        (quote!(always_recheck), "RADIX_RECHECK_ALWAYS"),
    ] {
        let options = parse_options(quote!(name = "support", semantic_revision = 1,
            for_function = "equal", operator_class = "key", max_spans = 4, max_output_bytes = 64, #policy)).unwrap();
        let generated = generate_planner_descriptor(
            &function, "support", &options, [1; 16], &functions, &classes,
        )
        .unwrap();
        syn::parse2::<syn::File>(generated.items).unwrap();
        syn::parse2::<syn::Expr>(generated.descriptor.clone()).unwrap();
        assert!(generated.descriptor.to_string().contains(constant));
        assert!(generate_planner_descriptor(
            &function,
            "support",
            &options,
            [1; 16],
            &BTreeMap::new(),
            &classes
        )
        .is_err());
        assert!(generate_planner_descriptor(
            &function,
            "support",
            &options,
            [1; 16],
            &functions,
            &BTreeMap::new()
        )
        .is_err());
    }
    for (policy, spans) in [
        (quote!(), 4u32),
        (quote!(exact, always_recheck), 4),
        (quote!(exact), 0),
        (quote!(exact), 4097),
    ] {
        let options = parse_options(quote!(name = "support", semantic_revision = 1,
            max_spans = #spans, max_output_bytes = 64, #policy))
        .unwrap();
        assert!(generate_planner_descriptor(
            &function, "support", &options, [1; 16], &functions, &classes
        )
        .is_err());
    }
}

#[test]
fn batch_adapter_requires_columns_context_and_cancellation_bound() {
    let function: ItemFn = syn::parse_quote!(
        fn batch(
            left: ColumnView<'_, i64>,
            right: ColumnView<'_, i64>,
            output: &mut Builder,
            context: &Context,
        ) {
        }
    );
    let options = parse_options(quote!(for_scalar = "sum", rows_per_cancel_check = 64)).unwrap();
    let result: Type = syn::parse_quote!(i64);
    let generated = generate_batch_wrapper(&function, &options, 2, &result).unwrap();
    syn::parse2::<syn::File>(generated.items.clone()).unwrap();
    assert_eq!(generated.wrapper.to_string(), "__radixdb_batch_batch");
    assert!(generated.items.to_string().contains("column_builder"));
    assert!(generate_batch_wrapper(&function, &options, 1, &result).is_err());
    let options = parse_options(quote!(rows_per_cancel_check = 0)).unwrap();
    assert!(generate_batch_wrapper(&function, &options, 2, &result).is_err());
}

#[test]
fn sql_rename_cannot_change_object_identity() {
    let package = [0x5a; 16];
    assert_eq!(
        object_id(package, "distance"),
        object_id(package, "distance")
    );
    assert_ne!(
        object_id(package, "distance"),
        object_id(package, "distance_v2")
    );
    assert_ne!(
        object_id(package, "distance"),
        object_id([0xa5; 16], "distance")
    );
}

#[test]
fn primitive_codecs_have_exact_width_and_reject_wrong_endianness() {
    for (name, codec, width) in [
        ("i8", "i8-le", 1),
        ("u8", "u8-le", 1),
        ("bool", "bool-u8", 1),
        ("i16", "i16-le", 2),
        ("u16", "u16-le", 2),
        ("i32", "i32-le", 4),
        ("u32", "u32-le", 4),
        ("f32", "f32-le", 4),
        ("i64", "i64-le", 8),
        ("u64", "u64-le", 8),
        ("f64", "f64-le", 8),
    ] {
        let ty: Type = syn::parse_str(name).unwrap();
        assert_eq!(primitive_codec_width(&ty), Some(width));
        validate_primitive_codec(&ty, codec, Span::call_site()).unwrap();
        assert!(validate_primitive_codec(&ty, "native-endian", Span::call_site()).is_err());
        let field = generate_field_codec(
            &format_ident!("value"),
            &ty,
            codec,
            &Options::default(),
            Span::call_site(),
        )
        .unwrap();
        assert_eq!(field.fixed_width, Some(width));
        assert_eq!(field.schema, codec);
        assert!(field.encode.to_string().contains("encode_field"));
        assert!(field.decode.to_string().contains("decode_field"));
    }
    let unsupported: Type = syn::parse_quote!(String);
    assert_eq!(primitive_codec_width(&unsupported), None);
    assert!(validate_primitive_codec(&unsupported, "utf8", Span::call_site()).is_err());
}

#[test]
fn sequence_and_array_codecs_enforce_explicit_bounds() {
    let field = format_ident!("values");
    let ty = syn::parse_quote!(Vec<u32>);
    for args in [quote!(), quote!(max_items = 4)] {
        assert!(generate_field_codec(
            &field,
            &ty,
            "u32-le",
            &parse_options(args).unwrap(),
            Span::call_site()
        )
        .is_err());
    }
    let bounds = parse_options(quote!(max_items = 4, max_bytes = 16)).unwrap();
    let sequence = generate_field_codec(&field, &ty, "u32-le", &bounds, Span::call_site()).unwrap();
    assert_eq!(sequence.fixed_width, None);
    assert_eq!(sequence.schema, "u32-le[max_items=4,max_bytes=16]");
    assert!(sequence.encode.to_string().contains("encode_sequence"));
    assert!(sequence.decode.to_string().contains("decode_sequence"));
    let array: Type = syn::parse_quote!([u32; 4]);
    assert!(generate_field_codec(&field, &array, "u32-le", &bounds, Span::call_site()).is_err());
    let fixed = generate_field_codec(
        &field,
        &array,
        "u32-le",
        &Options::default(),
        Span::call_site(),
    )
    .unwrap();
    assert_eq!(fixed.fixed_width, Some(16));
    for (array, codec) in [
        (syn::parse_quote!([u32; N]), "u32-le"),
        (syn::parse_quote!([u64; 4294967295]), "u64-le"),
    ] {
        assert!(generate_field_codec(
            &field,
            &array,
            codec,
            &Options::default(),
            Span::call_site()
        )
        .is_err());
    }
}

#[test]
fn derived_type_expansion_is_deterministic_and_valid_rust() {
    let input: DeriveInput = syn::parse_quote! {
        #[radix_type(id = "pair", name = "pair", codec = 1, semantic_revision = 1,
            storage = "fixed", max_bytes = 16, equality = equal, hash = hash, ordering = compare)]
        struct Pair {
            #[radix_field(codec = "i64-le")] left: i64,
            #[radix_field(codec = "i64-le")] right: i64,
        }
    };
    let output = expand_radix_type(input.clone()).unwrap();
    assert_eq!(
        output.to_string(),
        expand_radix_type(input).unwrap().to_string()
    );
    let parsed = syn::parse2::<syn::File>(output.clone()).unwrap();
    assert!(!parsed.items.is_empty());
    for name in [
        "encode",
        "decode",
        "test_corpus",
        "equal",
        "hash",
        "compare",
    ] {
        assert!(output.to_string().contains(name), "missing {name}");
    }
}

#[test]
fn package_expansion_retains_scalar_contract_and_entrypoint() {
    let args = quote!(
        id = "7c471f38-2224-431f-9248-c7443267411a",
        name = "proof",
        version = "1.0.0"
    );
    let module: ItemMod = syn::parse_quote! {
        mod proof {
            #[radixdb_scalar(id = "sum", name = "sum", semantic_revision = 1,
                immutable, strict, parallel_safe, cost = 1, cancellation = "bounded", max_output_bytes = 8)]
            fn sum(left: i64, right: i64) -> PluginResult<i64> { Ok(left + right) }
        }
    };
    let output = expand_plugin(parse_options(args.clone()).unwrap(), module.clone()).unwrap();
    assert_eq!(
        output.to_string(),
        expand_plugin(parse_options(args).unwrap(), module)
            .unwrap()
            .to_string()
    );
    let parsed = syn::parse2::<syn::File>(output.clone()).unwrap();
    assert!(!parsed.items.is_empty());
    for name in [
        "radixdb_plugin_entry_v1",
        "RadixPluginDescriptorV1",
        "__radixdb_descriptor",
        "sum",
    ] {
        assert!(output.to_string().contains(name), "missing {name}");
    }
}

#[test]
fn option_types_and_unknown_capabilities_fail_closed() {
    assert!(parse_options(quote!(name = "a", name = "b")).is_err());
    assert!(parse_options(quote!(nested(value))).is_err());
    let wrong = parse_options(quote!(name = 7, count = "7", callback = 7, unsafe_storage)).unwrap();
    assert!(wrong.string("name").is_err());
    assert!(wrong.u32("count").is_err());
    assert!(wrong.path("callback").is_err());
    assert!(wrong
        .reject_unknown(&["name", "count", "callback"], &[])
        .is_err());
    let valid = parse_options(quote!(
        name = "pair",
        count = 7,
        callback = module::call,
        strict
    ))
    .unwrap();
    assert_eq!(valid.string("name").unwrap().as_deref(), Some("pair"));
    assert_eq!(valid.u32("count").unwrap(), Some(7));
    assert!(valid.path("callback").unwrap().is_some());
    valid
        .reject_unknown(&["name", "count", "callback"], &["strict"])
        .unwrap();
    assert!(valid.required_string("missing", Span::call_site()).is_err());
    assert!(valid.required_u32("missing", Span::call_site()).is_err());
}
