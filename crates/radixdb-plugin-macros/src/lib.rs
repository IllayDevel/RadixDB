use std::collections::{BTreeMap, BTreeSet};

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use sha2::{Digest, Sha256};
use syn::{
    parse::Parser, parse_macro_input, punctuated::Punctuated, spanned::Spanned, Attribute, Data,
    DeriveInput, Expr, ExprLit, Fields, FnArg, GenericArgument, Item, ItemFn, ItemMod, Lit, Meta,
    PathArguments, ReturnType, Token, Type,
};

#[proc_macro_derive(RadixType, attributes(radix_type, radix_field))]
pub fn derive_radix_type(input: TokenStream) -> TokenStream {
    match expand_radix_type(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn radixdb_plugin(attribute: TokenStream, item: TokenStream) -> TokenStream {
    let args = match parse_options(attribute.into()) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    match expand_plugin(args, parse_macro_input!(item as ItemMod)) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

macro_rules! passthrough_attribute {
    ($name:ident) => {
        #[proc_macro_attribute]
        pub fn $name(_attribute: TokenStream, item: TokenStream) -> TokenStream {
            item
        }
    };
}

passthrough_attribute!(radixdb_scalar);
passthrough_attribute!(radixdb_batch);
passthrough_attribute!(radixdb_operator);
passthrough_attribute!(radixdb_operator_class);
passthrough_attribute!(radixdb_planner_support);

macro_rules! rejected_attribute {
    ($name:ident, $kind:literal) => {
        #[proc_macro_attribute]
        pub fn $name(_attribute: TokenStream, _item: TokenStream) -> TokenStream {
            syn::Error::new(
                Span::call_site(),
                concat!(
                    $kind,
                    " plugin functions are outside the RadixDB 1.2 authoring scope"
                ),
            )
            .into_compile_error()
            .into()
        }
    };
}

rejected_attribute!(radixdb_aggregate, "aggregate");
rejected_attribute!(radixdb_window, "window");
rejected_attribute!(radixdb_tvf, "table-valued");

#[derive(Default, Clone)]
struct Options {
    values: BTreeMap<String, Expr>,
    flags: BTreeSet<String>,
}

impl Options {
    fn required_string(&self, name: &str, span: Span) -> syn::Result<String> {
        self.string(name)?
            .ok_or_else(|| syn::Error::new(span, format!("missing required `{name}`")))
    }

    fn string(&self, name: &str) -> syn::Result<Option<String>> {
        self.values
            .get(name)
            .map(|value| match value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => Ok(value.value()),
                _ => Err(syn::Error::new(value.span(), "expected string literal")),
            })
            .transpose()
    }

    fn required_u32(&self, name: &str, span: Span) -> syn::Result<u32> {
        self.u32(name)?
            .ok_or_else(|| syn::Error::new(span, format!("missing required `{name}`")))
    }

    fn u32(&self, name: &str) -> syn::Result<Option<u32>> {
        self.values
            .get(name)
            .map(|value| match value {
                Expr::Lit(ExprLit {
                    lit: Lit::Int(value),
                    ..
                }) => value.base10_parse(),
                _ => Err(syn::Error::new(value.span(), "expected integer literal")),
            })
            .transpose()
    }

    fn path(&self, name: &str) -> syn::Result<Option<syn::Path>> {
        self.values
            .get(name)
            .map(|value| match value {
                Expr::Path(value) => Ok(value.path.clone()),
                _ => Err(syn::Error::new(value.span(), "expected callback path")),
            })
            .transpose()
    }

    fn reject_unknown(&self, allowed_values: &[&str], allowed_flags: &[&str]) -> syn::Result<()> {
        for name in self.values.keys() {
            if !allowed_values.contains(&name.as_str()) {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("unknown RadixDB attribute option `{name}`"),
                ));
            }
        }
        for name in &self.flags {
            if !allowed_flags.contains(&name.as_str()) {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("unknown RadixDB attribute flag `{name}`"),
                ));
            }
        }
        Ok(())
    }
}

fn parse_options(tokens: TokenStream2) -> syn::Result<Options> {
    let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(tokens)?;
    let mut result = Options::default();
    for meta in metas {
        match meta {
            Meta::Path(path) => {
                let name = single_ident(&path)?;
                if !result.flags.insert(name.clone()) {
                    return Err(syn::Error::new(path.span(), format!("duplicate `{name}`")));
                }
            }
            Meta::NameValue(value) => {
                let name = single_ident(&value.path)?;
                if result.values.insert(name.clone(), value.value).is_some() {
                    return Err(syn::Error::new(
                        value.path.span(),
                        format!("duplicate `{name}`"),
                    ));
                }
            }
            Meta::List(list) => {
                return Err(syn::Error::new(
                    list.span(),
                    "nested RadixDB attribute options are not supported",
                ));
            }
        }
    }
    Ok(result)
}

fn options_from_attribute(attribute: &Attribute) -> syn::Result<Options> {
    let Meta::List(list) = &attribute.meta else {
        return Err(syn::Error::new(
            attribute.span(),
            "expected attribute arguments",
        ));
    };
    parse_options(list.tokens.clone())
}

fn single_ident(path: &syn::Path) -> syn::Result<String> {
    if path.segments.len() != 1 {
        return Err(syn::Error::new(
            path.span(),
            "expected a single option name",
        ));
    }
    Ok(path.segments[0].ident.to_string())
}

fn attribute<'a>(attributes: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attributes
        .iter()
        .find(|attribute| attribute.path().is_ident(name))
}

fn expand_radix_type(input: DeriveInput) -> syn::Result<TokenStream2> {
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "RadixType does not support generic external types",
        ));
    }
    let type_attribute = attribute(&input.attrs, "radix_type")
        .ok_or_else(|| syn::Error::new(input.span(), "missing #[radix_type(...)]"))?;
    let options = options_from_attribute(type_attribute)?;
    options.reject_unknown(
        &[
            "id",
            "name",
            "codec",
            "semantic_revision",
            "storage",
            "max_bytes",
            "equality",
            "hash",
            "ordering",
            "manual",
        ],
        &[],
    )?;
    let local_id = options.required_string("id", input.span())?;
    let display_name = options.required_string("name", input.span())?;
    validate_local_id(&local_id, input.span())?;
    validate_sql_name(&display_name, input.span())?;
    let codec_version = options.required_u32("codec", input.span())?;
    let semantic_revision = options.required_u32("semantic_revision", input.span())?;
    if codec_version == 0 || semantic_revision == 0 {
        return Err(syn::Error::new(
            input.span(),
            "codec and semantic_revision must be non-zero",
        ));
    }
    let storage = options.required_string("storage", input.span())?;
    let max_bytes = options.required_u32("max_bytes", input.span())?;
    if max_bytes == 0 || max_bytes > 16 * 1024 * 1024 {
        return Err(syn::Error::new(
            input.span(),
            "max_bytes must be in 1..=16777216",
        ));
    }
    let storage_kind = match storage.as_str() {
        "fixed" => quote!(::radixdb_plugin::__private::abi::RADIX_EXTERNAL_STORAGE_FIXED),
        "variable" => quote!(::radixdb_plugin::__private::abi::RADIX_EXTERNAL_STORAGE_VARIABLE),
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "storage must be \"fixed\" or \"variable\"",
            ));
        }
    };
    let fixed_bytes = if storage == "fixed" { max_bytes } else { 0 };
    let equality = options.path("equality")?;
    let hash = options.path("hash")?;
    let ordering = options.path("ordering")?;
    if hash.is_some() && equality.is_none() {
        return Err(syn::Error::new(
            input.span(),
            "hash callback requires an explicit equality callback",
        ));
    }
    let manual = options.path("manual")?;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(input.span(), "RadixType requires a struct"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new(
            data.fields.span(),
            "RadixType requires named fields",
        ));
    };

    let (encode_body, decode_body, corpus_body, schema, inferred_fixed_bytes) = if let Some(codec) =
        manual
    {
        if fields
            .named
            .iter()
            .any(|field| attribute(&field.attrs, "radix_field").is_some())
        {
            return Err(syn::Error::new(
                fields.span(),
                "manual codec types must not declare radix_field codecs",
            ));
        }
        (
            quote!(<#codec as ::radixdb_plugin::ManualCodec<Self>>::encode(self, output)),
            quote!(<#codec as ::radixdb_plugin::ManualCodec<Self>>::decode(input)),
            quote!(<#codec as ::radixdb_plugin::ManualCodec<Self>>::corpus()),
            format!("manual:{}", quote!(#codec)),
            None,
        )
    } else {
        let mut encode = Vec::new();
        let mut decode = Vec::new();
        let mut names = Vec::new();
        let mut edge_extensions = Vec::new();
        let mut schema_parts = Vec::new();
        let mut encoded_width = Some(0_u32);
        for field in &fields.named {
            let field_ident = field.ident.as_ref().expect("named field");
            let field_type = &field.ty;
            names.push(field_ident.clone());
            let field_attribute = attribute(&field.attrs, "radix_field").ok_or_else(|| {
                syn::Error::new(
                    field.span(),
                    "every derived field requires #[radix_field(...)]",
                )
            })?;
            let field_options = options_from_attribute(field_attribute)?;
            field_options.reject_unknown(&["codec", "max_items", "max_bytes"], &[])?;
            let codec = field_options.required_string("codec", field.span())?;
            let generated =
                generate_field_codec(field_ident, &field.ty, &codec, &field_options, field.span())?;
            encode.push(generated.encode);
            decode.push(generated.decode);
            edge_extensions.push(generated.edge);
            encoded_width = match (encoded_width, generated.fixed_width) {
                (Some(total), Some(width)) => Some(total.checked_add(width).ok_or_else(|| {
                    syn::Error::new(field.span(), "derived fixed width exceeds u32")
                })?),
                _ => None,
            };
            schema_parts.push(format!(
                "{}:{}:{}",
                field_ident,
                quote!(#field_type),
                generated.schema
            ));
        }
        let defaults = names.iter().zip(fields.named.iter()).map(|(name, field)| {
            let ty = &field.ty;
            quote!(#name: <#ty as ::std::default::Default>::default())
        });
        (
            quote!({ #(#encode)* Ok(()) }),
            quote!({ #(let #names = #decode;)* Ok(Self { #(#names),* }) }),
            quote!({
                let mut corpus = vec![Self { #(#defaults),* }];
                #(#edge_extensions)*
                corpus
            }),
            schema_parts.join(";"),
            encoded_width,
        )
    };
    if storage == "fixed" && inferred_fixed_bytes.is_some_and(|encoded| encoded != max_bytes) {
        return Err(syn::Error::new(
            input.span(),
            format!(
                "fixed type max_bytes must equal derived encoded width {}",
                inferred_fixed_bytes.unwrap()
            ),
        ));
    }

    let mut capabilities = 0u64;
    if equality.is_some() {
        capabilities |= 1 << 0;
    }
    if hash.is_some() {
        capabilities |= 1 << 1;
    }
    if ordering.is_some() {
        capabilities |= 1 << 2;
    }
    // This fingerprint is persisted beside encoded values. It identifies only
    // the physical codec: SQL names and semantic callbacks can evolve without
    // forcing a codec migration. Their evolution is tracked independently by
    // semantic revisions and the package descriptor fingerprint.
    let fingerprint = digest32(format!(
        "radixdb.type.codec.v1\0{id}\0{codec_version}\0{storage}\0{max_bytes}\0{schema}",
        id = local_id,
    ));
    let fingerprint_tokens = bytes_tokens(&fingerprint);
    let encode_wrapper =
        format_ident!("__radixdb_type_{}_encode", ident.to_string().to_lowercase());
    let decode_wrapper =
        format_ident!("__radixdb_type_{}_decode", ident.to_string().to_lowercase());
    let equal_wrapper = format_ident!("__radixdb_type_{}_equal", ident.to_string().to_lowercase());
    let hash_wrapper = format_ident!("__radixdb_type_{}_hash", ident.to_string().to_lowercase());
    let compare_wrapper = format_ident!(
        "__radixdb_type_{}_compare",
        ident.to_string().to_lowercase()
    );

    let equality_impl = if let Some(callback) = &equality {
        quote! {
            fn semantic_equal(left: &Self, right: &Self) -> Option<bool> {
                let callback: fn(&Self, &Self) -> bool = #callback;
                Some(callback(left, right))
            }
        }
    } else {
        quote!()
    };
    let hash_impl = if let Some(callback) = &hash {
        quote! {
            fn semantic_hash(
                value: &Self,
                sink: &mut ::radixdb_plugin::HashSink<'_>,
            ) -> Option<::radixdb_plugin::PluginResult<()>> {
                let callback: fn(&Self, &mut ::radixdb_plugin::HashSink<'_>)
                    -> ::radixdb_plugin::PluginResult<()> = #callback;
                Some(callback(value, sink))
            }
        }
    } else {
        quote!()
    };
    let ordering_impl = if let Some(callback) = &ordering {
        quote! {
            fn semantic_compare(left: &Self, right: &Self) -> Option<::std::cmp::Ordering> {
                let callback: fn(&Self, &Self) -> ::std::cmp::Ordering = #callback;
                Some(callback(left, right))
            }
        }
    } else {
        quote!()
    };
    let equal_items = if equality.is_some() {
        quote! {
            unsafe extern "C" fn #equal_wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                left: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                right: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                output: *mut u8,
            ) -> u32 {
                unsafe { ::radixdb_plugin::__private::run_equal::<#ident>(context, left, right, output) }
            }
        }
    } else {
        quote!()
    };
    let hash_items = if hash.is_some() {
        quote! {
            unsafe extern "C" fn #hash_wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                value: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                sink: *const ::radixdb_plugin::__private::abi::RadixAbiHashSinkV1,
            ) -> u32 {
                unsafe { ::radixdb_plugin::__private::run_hash::<#ident>(context, value, sink) }
            }
        }
    } else {
        quote!()
    };
    let ordering_items = if ordering.is_some() {
        quote! {
            unsafe extern "C" fn #compare_wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                left: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                right: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                output: *mut i8,
            ) -> u32 {
                unsafe { ::radixdb_plugin::__private::run_compare::<#ident>(context, left, right, output) }
            }
        }
    } else {
        quote!()
    };
    let equal_const = equality
        .as_ref()
        .map(|_| quote!(Some(#equal_wrapper)))
        .unwrap_or(quote!(None));
    let hash_const = hash
        .as_ref()
        .map(|_| quote!(Some(#hash_wrapper)))
        .unwrap_or(quote!(None));
    let ordering_const = ordering
        .as_ref()
        .map(|_| quote!(Some(#compare_wrapper)))
        .unwrap_or(quote!(None));

    Ok(quote! {
        unsafe extern "C" fn #encode_wrapper(
            context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
            input: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
            output: *const ::radixdb_plugin::__private::abi::RadixAbiResultBuilderV1,
        ) -> u32 {
            unsafe { ::radixdb_plugin::__private::run_codec_encode::<#ident>(context, input, output) }
        }

        unsafe extern "C" fn #decode_wrapper(
            context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
            input: ::radixdb_plugin::__private::abi::RadixAbiSliceV1,
            output: *const ::radixdb_plugin::__private::abi::RadixAbiResultBuilderV1,
        ) -> u32 {
            unsafe { ::radixdb_plugin::__private::run_codec_decode::<#ident>(context, input, output) }
        }

        #equal_items
        #hash_items
        #ordering_items

        impl ::radixdb_plugin::RadixType for #ident {
            const LOCAL_ID: &'static str = #local_id;
            const DISPLAY_NAME: &'static str = #display_name;
            const CODEC_VERSION: u32 = #codec_version;
            const SEMANTIC_REVISION: u32 = #semantic_revision;
            const STORAGE_KIND: u16 = #storage_kind;
            const FIXED_BYTES: u32 = #fixed_bytes;
            const MAX_BYTES: u32 = #max_bytes;
            const CAPABILITIES: u64 = #capabilities;
            const CODEC_FINGERPRINT: [u8; 32] = [#(#fingerprint_tokens),*];
            const ABI_ENCODE: Option<::radixdb_plugin::__private::abi::RadixAbiCodecFnV1> =
                Some(#encode_wrapper);
            const ABI_DECODE: Option<::radixdb_plugin::__private::abi::RadixAbiParseFnV1> =
                Some(#decode_wrapper);
            const ABI_EQUALITY: Option<::radixdb_plugin::__private::abi::RadixAbiEqualFnV1> =
                #equal_const;
            const ABI_HASH: Option<::radixdb_plugin::__private::abi::RadixAbiHashFnV1> =
                #hash_const;
            const ABI_ORDERING: Option<::radixdb_plugin::__private::abi::RadixAbiCompareFnV1> =
                #ordering_const;

            fn encode(&self, output: &mut ::radixdb_plugin::CodecWriter)
                -> ::radixdb_plugin::PluginResult<()>
            {
                #encode_body
            }

            fn decode(input: &mut ::radixdb_plugin::CodecReader<'_>)
                -> ::radixdb_plugin::PluginResult<Self>
            {
                #decode_body
            }

            #[allow(clippy::needless_update)]
            fn test_corpus() -> Vec<Self> {
                #corpus_body
            }

            #equality_impl
            #hash_impl
            #ordering_impl
        }
    })
}

struct GeneratedField {
    encode: TokenStream2,
    decode: TokenStream2,
    edge: TokenStream2,
    schema: String,
    fixed_width: Option<u32>,
}

fn generate_field_codec(
    field: &Ident,
    ty: &Type,
    codec: &str,
    options: &Options,
    span: Span,
) -> syn::Result<GeneratedField> {
    if let Some(inner) = vec_inner(ty) {
        let max_items = options
            .u32("max_items")?
            .ok_or_else(|| syn::Error::new(span, "bounded sequence requires max_items"))?;
        let max_bytes = options
            .u32("max_bytes")?
            .ok_or_else(|| syn::Error::new(span, "bounded sequence requires max_bytes"))?;
        validate_primitive_codec(inner, codec, span)?;
        return Ok(GeneratedField {
            encode: quote! {
                ::radixdb_plugin::__private::encode_sequence(
                    &self.#field,
                    #max_items as usize,
                    #max_bytes as usize,
                    output,
                )?;
            },
            decode: quote!(::radixdb_plugin::__private::decode_sequence::<#inner>(
                input,
                #max_items as usize,
                #max_bytes as usize,
            )?),
            edge: quote! {
                for value in <#inner as ::radixdb_plugin::__private::CanonicalField>::edge_values() {
                    corpus.push(Self {
                        #field: vec![value.clone()],
                        ..Self::default()
                    });
                    corpus.push(Self {
                        #field: vec![value; #max_items as usize],
                        ..Self::default()
                    });
                }
            },
            schema: format!("{codec}[max_items={max_items},max_bytes={max_bytes}]"),
            fixed_width: None,
        });
    }
    if options.values.contains_key("max_items") || options.values.contains_key("max_bytes") {
        return Err(syn::Error::new(
            span,
            "max_items/max_bytes are valid only for bounded sequences",
        ));
    }
    if let Type::Array(array) = ty {
        validate_primitive_codec(&array.elem, codec, span)?;
        let length = match &array.len {
            Expr::Lit(ExprLit {
                lit: Lit::Int(length),
                ..
            }) => length.base10_parse::<u32>()?,
            _ => {
                return Err(syn::Error::new(
                    array.len.span(),
                    "fixed array length must be an integer literal",
                ));
            }
        };
        let element_width = primitive_codec_width(&array.elem).ok_or_else(|| {
            syn::Error::new(array.elem.span(), "unsupported fixed-array element type")
        })?;
        return Ok(GeneratedField {
            encode: quote!(::radixdb_plugin::__private::CanonicalField::encode_field(
                &self.#field,
                output,
            )?;),
            decode: quote!(<#ty as ::radixdb_plugin::__private::CanonicalField>::decode_field(input)?),
            edge: quote! {
                for value in <#ty as ::radixdb_plugin::__private::CanonicalField>::edge_values() {
                    corpus.push(Self {
                        #field: value,
                        ..Self::default()
                    });
                }
            },
            schema: format!("{codec}[{}]", quote!(#array.len)),
            fixed_width: Some(length.checked_mul(element_width).ok_or_else(|| {
                syn::Error::new(array.len.span(), "fixed array width exceeds u32")
            })?),
        });
    }
    validate_primitive_codec(ty, codec, span)?;
    Ok(GeneratedField {
        encode: quote!(::radixdb_plugin::__private::CanonicalField::encode_field(
            &self.#field,
            output,
        )?;),
        decode: quote!(<#ty as ::radixdb_plugin::__private::CanonicalField>::decode_field(input)?),
        edge: quote! {
            for value in <#ty as ::radixdb_plugin::__private::CanonicalField>::edge_values() {
                corpus.push(Self {
                    #field: value,
                    ..Self::default()
                });
            }
        },
        schema: codec.to_string(),
        fixed_width: primitive_codec_width(ty),
    })
}

fn primitive_codec_width(ty: &Type) -> Option<u32> {
    match terminal_type_ident(ty)?.as_str() {
        "i8" | "u8" | "bool" => Some(1),
        "i16" | "u16" => Some(2),
        "i32" | "u32" | "f32" => Some(4),
        "i64" | "u64" | "f64" => Some(8),
        _ => None,
    }
}

fn validate_primitive_codec(ty: &Type, codec: &str, span: Span) -> syn::Result<()> {
    let expected = match terminal_type_ident(ty).as_deref() {
        Some("i8") => "i8-le",
        Some("i16") => "i16-le",
        Some("i32") => "i32-le",
        Some("i64") => "i64-le",
        Some("u8") => "u8-le",
        Some("u16") => "u16-le",
        Some("u32") => "u32-le",
        Some("u64") => "u64-le",
        Some("f32") => "f32-le",
        Some("f64") => "f64-le",
        Some("bool") => "bool-u8",
        _ => {
            return Err(syn::Error::new(
                span,
                "unsupported field type; use a primitive, fixed array, bounded Vec, or manual codec",
            ));
        }
    };
    if codec != expected {
        return Err(syn::Error::new(
            span,
            format!("field type requires codec \"{expected}\""),
        ));
    }
    Ok(())
}

fn expand_plugin(args: Options, mut module: ItemMod) -> syn::Result<TokenStream2> {
    args.reject_unknown(&["id", "name", "version"], &[])?;
    let package_id = args.required_string("id", module.span())?;
    let package_name = args.required_string("name", module.span())?;
    let package_version = args.required_string("version", module.span())?;
    let uuid = uuid::Uuid::parse_str(&package_id)
        .map_err(|_| syn::Error::new(module.span(), "plugin id must be a canonical UUID"))?;
    if uuid.to_string() != package_id {
        return Err(syn::Error::new(
            module.span(),
            "plugin id must use canonical lowercase UUID spelling",
        ));
    }
    validate_package_name(&package_name, module.span())?;
    let version = semver::Version::parse(&package_version)
        .map_err(|_| syn::Error::new(module.span(), "plugin version must be SemVer"))?;
    if version.to_string() != package_version || !version.build.is_empty() {
        return Err(syn::Error::new(
            module.span(),
            "plugin version must be canonical SemVer without build metadata",
        ));
    }
    let Some((_, items)) = &mut module.content else {
        return Err(syn::Error::new(
            module.span(),
            "radixdb_plugin requires an inline module",
        ));
    };

    let mut type_specs = Vec::new();
    let mut scalar_specs = Vec::new();
    let mut batch_specs = BTreeMap::new();
    let mut operator_specs = Vec::new();
    let mut opclass_specs = Vec::new();
    let mut planner_specs = Vec::new();
    let mut local_ids = BTreeSet::new();
    for item in items.iter() {
        match item {
            Item::Struct(item) => {
                if let Some(attribute) = attribute(&item.attrs, "radix_type") {
                    let options = options_from_attribute(attribute)?;
                    let local_id = options.required_string("id", item.span())?;
                    admit_local_id(&mut local_ids, &local_id, item.span())?;
                    type_specs.push((item.ident.clone(), local_id, options));
                }
            }
            Item::Fn(function) => {
                if let Some(attribute) = attribute(&function.attrs, "radixdb_scalar") {
                    let options = options_from_attribute(attribute)?;
                    let local_id = options.required_string("id", function.span())?;
                    admit_local_id(&mut local_ids, &local_id, function.span())?;
                    scalar_specs.push((function.clone(), local_id, options));
                }
                if let Some(attribute) = attribute(&function.attrs, "radixdb_batch") {
                    let options = options_from_attribute(attribute)?;
                    let scalar = options.required_string("for_scalar", function.span())?;
                    if batch_specs
                        .insert(scalar.clone(), (function.clone(), options))
                        .is_some()
                    {
                        return Err(syn::Error::new(
                            function.span(),
                            format!("duplicate batch adapter for `{scalar}`"),
                        ));
                    }
                }
                if let Some(attribute) = attribute(&function.attrs, "radixdb_operator") {
                    let options = options_from_attribute(attribute)?;
                    let local_id = options.required_string("id", function.span())?;
                    admit_local_id(&mut local_ids, &local_id, function.span())?;
                    operator_specs.push((function.clone(), local_id, options));
                }
                if let Some(attribute) = attribute(&function.attrs, "radixdb_operator_class") {
                    let options = options_from_attribute(attribute)?;
                    let local_id = options.required_string("id", function.span())?;
                    admit_local_id(&mut local_ids, &local_id, function.span())?;
                    opclass_specs.push((function.clone(), local_id, options));
                }
                if let Some(attribute) = attribute(&function.attrs, "radixdb_planner_support") {
                    let options = options_from_attribute(attribute)?;
                    let local_id = options.required_string("id", function.span())?;
                    admit_local_id(&mut local_ids, &local_id, function.span())?;
                    planner_specs.push((function.clone(), local_id, options));
                }
            }
            _ => {}
        }
    }

    let package_uuid = *uuid.as_bytes();
    let type_map: BTreeMap<String, ([u8; 16], u32)> = type_specs
        .iter()
        .map(|(ident, local_id, options)| {
            Ok((
                ident.to_string(),
                (
                    object_id(package_uuid, local_id),
                    options.required_u32("codec", ident.span())?,
                ),
            ))
        })
        .collect::<syn::Result<_>>()?;
    let mut type_names = BTreeSet::new();
    for (ident, _, options) in &type_specs {
        let name = options.required_string("name", ident.span())?;
        if !type_names.insert(name.clone()) {
            return Err(syn::Error::new(
                ident.span(),
                format!("duplicate external type SQL name `{name}`"),
            ));
        }
    }
    let mut overloads = BTreeSet::new();
    for (function, _, options) in &scalar_specs {
        let name = options.required_string("name", function.span())?;
        let arguments = function_arguments(function)?;
        let signature = format!(
            "{}({})",
            name,
            arguments
                .iter()
                .map(|(_, ty)| quote!(#ty).to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        if !overloads.insert(signature.clone()) {
            return Err(syn::Error::new(
                function.span(),
                format!("duplicate scalar overload `{signature}`"),
            ));
        }
    }
    let mut operator_overloads = BTreeSet::new();
    for (function, _, options) in &operator_specs {
        let symbol = options.required_string("symbol", function.span())?;
        let left = parse_type_option(options, "left", function.span())?;
        let right = parse_type_option(options, "right", function.span())?;
        let signature = format!("{}({},{})", symbol, quote!(#left), quote!(#right));
        if !operator_overloads.insert(signature.clone()) {
            return Err(syn::Error::new(
                function.span(),
                format!("duplicate operator overload `{signature}`"),
            ));
        }
    }
    let function_ids: BTreeMap<String, [u8; 16]> = scalar_specs
        .iter()
        .map(|(_, local_id, _)| (local_id.clone(), object_id(package_uuid, local_id)))
        .chain(scalar_specs.iter().map(|(function, local_id, _)| {
            (
                function.sig.ident.to_string(),
                object_id(package_uuid, local_id),
            )
        }))
        .collect();
    let operator_ids: BTreeMap<(String, String, String), [u8; 16]> = operator_specs
        .iter()
        .map(|(function, local_id, options)| {
            let symbol = options.required_string("symbol", function.span())?;
            let left = parse_type_option(options, "left", function.span())?;
            let right = parse_type_option(options, "right", function.span())?;
            Ok((
                (
                    symbol,
                    quote!(#left).to_string(),
                    quote!(#right).to_string(),
                ),
                object_id(package_uuid, local_id),
            ))
        })
        .collect::<syn::Result<_>>()?;
    let opclass_ids: BTreeMap<String, [u8; 16]> = opclass_specs
        .iter()
        .map(|(function, local_id, _)| {
            (
                function.sig.ident.to_string(),
                object_id(package_uuid, local_id),
            )
        })
        .chain(
            opclass_specs
                .iter()
                .map(|(_, local_id, _)| (local_id.clone(), object_id(package_uuid, local_id))),
        )
        .collect();

    let type_descriptors = type_specs
        .iter()
        .map(|(ident, local_id, _)| generate_type_descriptor(ident, package_uuid, local_id));
    let has_batch = !batch_specs.is_empty();
    let mut generated_functions = Vec::new();
    let mut function_descriptors = Vec::new();
    for (function, local_id, options) in &scalar_specs {
        let batch = batch_specs
            .remove(local_id)
            .or_else(|| batch_specs.remove(&function.sig.ident.to_string()));
        let generated = generate_scalar_descriptor(
            function,
            local_id,
            options,
            batch.as_ref(),
            package_uuid,
            &type_map,
        )?;
        generated_functions.push(generated.items);
        function_descriptors.push(generated.descriptor);
    }
    if let Some((name, (function, _))) = batch_specs.into_iter().next() {
        return Err(syn::Error::new(
            function.span(),
            format!("batch adapter references unknown scalar `{name}`"),
        ));
    }
    let operator_descriptors = operator_specs
        .iter()
        .map(|(function, local_id, options)| {
            generate_operator_descriptor(
                function,
                local_id,
                options,
                package_uuid,
                &type_map,
                &function_ids,
            )
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let mut opclass_items = Vec::new();
    let mut opclass_descriptors = Vec::new();
    for (function, local_id, options) in &opclass_specs {
        let generated = generate_opclass_descriptor(
            function,
            local_id,
            options,
            package_uuid,
            &type_map,
            &operator_ids,
        )?;
        opclass_items.push(generated.items);
        opclass_descriptors.push(generated.descriptor);
    }
    let mut planner_items = Vec::new();
    let mut planner_descriptors = Vec::new();
    for (function, local_id, options) in &planner_specs {
        let generated = generate_planner_descriptor(
            function,
            local_id,
            options,
            package_uuid,
            &function_ids,
            &opclass_ids,
        )?;
        planner_items.push(generated.items);
        planner_descriptors.push(generated.descriptor);
    }

    let package_caps: u64 = (if !type_specs.is_empty() { 1 } else { 0 })
        | (if !scalar_specs.is_empty() { 1 << 1 } else { 0 })
        | (if has_batch { 1 << 2 } else { 0 })
        | (if !operator_specs.is_empty() {
            1 << 3
        } else {
            0
        })
        | (if !opclass_specs.is_empty() { 1 << 4 } else { 0 })
        | (if !planner_specs.is_empty() { 1 << 5 } else { 0 });

    let source_contract = quote!(#(#items)*).to_string();
    let descriptor_fingerprint = digest32(format!(
        "radixdb.package.v1\0{}\0{}\0{}\0{}",
        package_id,
        package_name,
        local_ids.iter().cloned().collect::<Vec<_>>().join("\0"),
        source_contract,
    ));
    let fingerprint_tokens = bytes_tokens(&descriptor_fingerprint);
    let package_id_tokens = bytes_tokens(&package_uuid);
    let type_count = type_specs.len() as u32;
    let function_count = scalar_specs.len() as u32;
    let operator_count = operator_specs.len() as u32;
    let opclass_count = opclass_specs.len() as u32;
    let planner_count = planner_specs.len() as u32;

    let generated = quote! {
        #(#generated_functions)*
        #(#opclass_items)*
        #(#planner_items)*

        static __RADIXDB_TYPES: [::radixdb_plugin::__private::abi::RadixAbiExternalTypeDescriptorV1; #type_count as usize] = [
            #(#type_descriptors),*
        ];
        static __RADIXDB_FUNCTIONS: [::radixdb_plugin::__private::abi::RadixAbiScalarFunctionDescriptorV1; #function_count as usize] = [
            #(#function_descriptors),*
        ];
        static __RADIXDB_OPERATORS: [::radixdb_plugin::__private::abi::RadixAbiOperatorDescriptorV1; #operator_count as usize] = [
            #(#operator_descriptors),*
        ];
        static __RADIXDB_OPERATOR_CLASSES: [::radixdb_plugin::__private::abi::RadixAbiOperatorClassDescriptorV1; #opclass_count as usize] = [
            #(#opclass_descriptors),*
        ];
        static __RADIXDB_PLANNER_SUPPORT: [::radixdb_plugin::__private::abi::RadixAbiPlannerSupportDescriptorV1; #planner_count as usize] = [
            #(#planner_descriptors),*
        ];
        static __RADIXDB_PACKAGE: ::radixdb_plugin::__private::abi::RadixPluginDescriptorV1 =
            ::radixdb_plugin::__private::abi::RadixPluginDescriptorV1 {
                header: ::radixdb_plugin::__private::abi::RadixAbiHeaderV1::new::<
                    ::radixdb_plugin::__private::abi::RadixPluginDescriptorV1
                >(#package_caps),
                package_id: [#(#package_id_tokens),*],
                package_name: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#package_name.as_bytes()),
                package_version: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#package_version.as_bytes()),
                abi_min_minor: ::radixdb_plugin::__private::abi::RADIX_ABI_MINOR,
                abi_max_minor: ::radixdb_plugin::__private::abi::RADIX_ABI_MINOR,
                reserved: 0,
                descriptor_fingerprint: [#(#fingerprint_tokens),*],
                type_count: #type_count,
                reserved_types: 0,
                types: __RADIXDB_TYPES.as_ptr(),
                function_count: #function_count,
                reserved_functions: 0,
                functions: __RADIXDB_FUNCTIONS.as_ptr(),
                operator_count: #operator_count,
                reserved_operators: 0,
                operators: __RADIXDB_OPERATORS.as_ptr(),
                operator_class_count: #opclass_count,
                reserved_operator_classes: 0,
                operator_classes: __RADIXDB_OPERATOR_CLASSES.as_ptr(),
                planner_support_count: #planner_count,
                reserved_planner_support: 0,
                planner_support: __RADIXDB_PLANNER_SUPPORT.as_ptr(),
            };

        #[doc(hidden)]
        pub fn __radixdb_descriptor() -> &'static ::radixdb_plugin::__private::abi::RadixPluginDescriptorV1 {
            &__RADIXDB_PACKAGE
        }
    };
    let original_items = items.clone();
    let attrs = &module.attrs;
    let visibility = &module.vis;
    let module_ident = &module.ident;
    Ok(quote! {
        #(#attrs)* #visibility mod #module_ident {
            #(#original_items)*
            #generated
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn radixdb_plugin_entry_v1(
            host: *const ::radixdb_plugin::__private::abi::RadixHostApiV1,
            status: *mut u32,
        ) -> *const ::radixdb_plugin::__private::abi::RadixPluginDescriptorV1 {
            let outcome = ::std::panic::catch_unwind(|| {
                let host = unsafe { host.as_ref() }.ok_or(())?;
                if host.header.abi_major != ::radixdb_plugin::__private::abi::RADIX_ABI_MAJOR
                    || host.header.abi_minor < ::radixdb_plugin::__private::abi::RADIX_ABI_MINOR
                {
                    return Err(());
                }
                Ok(#module_ident::__radixdb_descriptor() as *const _)
            });
            match outcome {
                Ok(Ok(descriptor)) => {
                    if let Some(status) = unsafe { status.as_mut() } {
                        *status = ::radixdb_plugin::__private::abi::RADIX_STATUS_OK;
                    }
                    descriptor
                }
                Ok(Err(())) => {
                    if let Some(status) = unsafe { status.as_mut() } {
                        *status = ::radixdb_plugin::__private::abi::RADIX_STATUS_UNSUPPORTED_ABI;
                    }
                    ::std::ptr::null()
                }
                Err(_) => {
                    if let Some(status) = unsafe { status.as_mut() } {
                        *status = ::radixdb_plugin::__private::abi::RADIX_STATUS_PANIC;
                    }
                    ::std::ptr::null()
                }
            }
        }
    })
}

fn generate_type_descriptor(ident: &Ident, package_id: [u8; 16], local_id: &str) -> TokenStream2 {
    let object = bytes_tokens(&object_id(package_id, local_id));
    quote! {
        ::radixdb_plugin::__private::abi::RadixAbiExternalTypeDescriptorV1 {
            header: ::radixdb_plugin::__private::abi::RadixAbiHeaderV1::new::<
                ::radixdb_plugin::__private::abi::RadixAbiExternalTypeDescriptorV1
            >(0),
            object_id: [#(#object),*],
            local_id: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(
                <#ident as ::radixdb_plugin::RadixType>::LOCAL_ID.as_bytes()
            ),
            display_name: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(
                <#ident as ::radixdb_plugin::RadixType>::DISPLAY_NAME.as_bytes()
            ),
            codec_version: <#ident as ::radixdb_plugin::RadixType>::CODEC_VERSION,
            semantic_revision: <#ident as ::radixdb_plugin::RadixType>::SEMANTIC_REVISION,
            storage_kind: <#ident as ::radixdb_plugin::RadixType>::STORAGE_KIND,
            reserved_u16: 0,
            fixed_bytes: <#ident as ::radixdb_plugin::RadixType>::FIXED_BYTES,
            max_bytes: <#ident as ::radixdb_plugin::RadixType>::MAX_BYTES,
            reserved_u32: 0,
            capabilities: <#ident as ::radixdb_plugin::RadixType>::CAPABILITIES,
            codec_fingerprint: <#ident as ::radixdb_plugin::RadixType>::CODEC_FINGERPRINT,
            encode: <#ident as ::radixdb_plugin::RadixType>::ABI_ENCODE,
            decode: <#ident as ::radixdb_plugin::RadixType>::ABI_DECODE,
            equality: <#ident as ::radixdb_plugin::RadixType>::ABI_EQUALITY,
            hash: <#ident as ::radixdb_plugin::RadixType>::ABI_HASH,
            ordering: <#ident as ::radixdb_plugin::RadixType>::ABI_ORDERING,
            text_input: None,
            text_output: None,
            binary_input: None,
            binary_output: None,
        }
    }
}

struct GeneratedDescriptor {
    items: TokenStream2,
    descriptor: TokenStream2,
}

fn generate_scalar_descriptor(
    function: &ItemFn,
    local_id: &str,
    options: &Options,
    batch: Option<&(ItemFn, Options)>,
    package_id: [u8; 16],
    type_map: &BTreeMap<String, ([u8; 16], u32)>,
) -> syn::Result<GeneratedDescriptor> {
    options.reject_unknown(
        &[
            "id",
            "name",
            "semantic_revision",
            "cost",
            "cancellation",
            "max_output_bytes",
        ],
        &["immutable", "stable", "volatile", "strict", "parallel_safe"],
    )?;
    let name = options.required_string("name", function.span())?;
    validate_sql_name(&name, function.span())?;
    let semantic_revision = options.required_u32("semantic_revision", function.span())?;
    let cost = options.required_u32("cost", function.span())?;
    if semantic_revision == 0 || cost == 0 {
        return Err(syn::Error::new(
            function.span(),
            "semantic_revision and cost must be non-zero",
        ));
    }
    let volatility_flags = ["immutable", "stable", "volatile"]
        .into_iter()
        .filter(|flag| options.flags.contains(*flag))
        .collect::<Vec<_>>();
    if volatility_flags.len() != 1 {
        return Err(syn::Error::new(
            function.span(),
            "scalar requires exactly one of immutable, stable, volatile",
        ));
    }
    let volatility = match volatility_flags[0] {
        "immutable" => quote!(::radixdb_plugin::__private::abi::RADIX_VOLATILITY_IMMUTABLE),
        "stable" => quote!(::radixdb_plugin::__private::abi::RADIX_VOLATILITY_STABLE),
        _ => quote!(::radixdb_plugin::__private::abi::RADIX_VOLATILITY_VOLATILE),
    };
    let cancellation = options.required_string("cancellation", function.span())?;
    if cancellation != "bounded" {
        return Err(syn::Error::new(
            function.span(),
            "RadixDB 1.2 scalar cancellation must be \"bounded\"",
        ));
    }
    let arguments = function_arguments(function)?;
    let result = plugin_result_type(&function.sig.output)?;
    let argument_refs = arguments
        .iter()
        .map(|(_, ty)| type_ref_tokens(ty, type_map))
        .collect::<syn::Result<Vec<_>>>()?;
    let result_ref = type_ref_tokens(&result, type_map)?;
    let argument_count = arguments.len() as u32;
    let function_ident = &function.sig.ident;
    let wrapper = format_ident!("__radixdb_scalar_{}", function_ident);
    let argument_static = format_ident!(
        "__RADIXDB_ARGS_{}",
        function_ident.to_string().to_uppercase()
    );
    let decoded = arguments.iter().enumerate().map(|(index, (ident, ty))| {
        quote!(let #ident: #ty = <#ty as ::radixdb_plugin::ValueType>::decode_abi(&__arguments[#index])?;)
    });
    let call_arguments = arguments.iter().map(|(ident, _)| ident);
    let strict = options.flags.contains("strict");
    let parallel_safe = options.flags.contains("parallel_safe");
    let max_output = options
        .u32("max_output_bytes")?
        .map(|value| quote!(#value))
        .unwrap_or_else(|| quote!(<#result as ::radixdb_plugin::ValueType>::MAX_OUTPUT_BYTES));

    let (batch_items, batch_pointer) = if let Some((batch, batch_options)) = batch {
        let generated = generate_batch_wrapper(batch, batch_options, argument_count, &result)?;
        let wrapper = generated.wrapper;
        (generated.items, quote!(Some(#wrapper)))
    } else {
        (quote!(), quote!(None))
    };
    let object = bytes_tokens(&object_id(package_id, local_id));
    Ok(GeneratedDescriptor {
        items: quote! {
            static #argument_static: [::radixdb_plugin::__private::abi::RadixAbiTypeRefV1; #argument_count as usize] = [
                #(#argument_refs),*
            ];
            unsafe extern "C" fn #wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                arguments: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                argument_count: u32,
                output: *const ::radixdb_plugin::__private::abi::RadixAbiResultBuilderV1,
            ) -> u32 {
                unsafe {
                    ::radixdb_plugin::__private::run_scalar(
                        context,
                        arguments,
                        argument_count,
                        output,
                        #argument_count,
                        #strict,
                        |_context, __arguments, __output| {
                            #(#decoded)*
                            let __result: #result = #function_ident(#(#call_arguments),*)?;
                            __output.push(__result)
                        },
                    )
                }
            }
            #batch_items
        },
        descriptor: quote! {
            ::radixdb_plugin::__private::abi::RadixAbiScalarFunctionDescriptorV1 {
                header: ::radixdb_plugin::__private::abi::RadixAbiHeaderV1::new::<
                    ::radixdb_plugin::__private::abi::RadixAbiScalarFunctionDescriptorV1
                >(0),
                object_id: [#(#object),*],
                local_id: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#local_id.as_bytes()),
                display_name: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#name.as_bytes()),
                semantic_revision: #semantic_revision,
                argument_count: #argument_count,
                arguments: #argument_static.as_ptr(),
                result: #result_ref,
                volatility: #volatility,
                cancellation: ::radixdb_plugin::__private::abi::RADIX_CANCELLATION_BOUNDED,
                strict: #strict as u8,
                parallel_safe: #parallel_safe as u8,
                reserved_u16: 0,
                cost: #cost,
                max_output_bytes: #max_output,
                scalar: Some(#wrapper),
                batch: #batch_pointer,
            }
        },
    })
}

struct GeneratedBatch {
    items: TokenStream2,
    wrapper: Ident,
}

fn generate_batch_wrapper(
    function: &ItemFn,
    options: &Options,
    expected_columns: u32,
    result: &Type,
) -> syn::Result<GeneratedBatch> {
    options.reject_unknown(&["for_scalar", "rows_per_cancel_check"], &[])?;
    let rows = options.required_u32("rows_per_cancel_check", function.span())?;
    if rows == 0 {
        return Err(syn::Error::new(
            function.span(),
            "rows_per_cancel_check must be non-zero",
        ));
    }
    let inputs = function
        .sig
        .inputs
        .iter()
        .take(expected_columns as usize)
        .enumerate()
        .map(|(index, argument)| {
            let FnArg::Typed(argument) = argument else {
                return Err(syn::Error::new(
                    argument.span(),
                    "methods are not supported",
                ));
            };
            let inner = generic_inner(&argument.ty, "ColumnView").ok_or_else(|| {
                syn::Error::new(argument.ty.span(), "batch inputs must be ColumnView<'_, T>")
            })?;
            let name = format_ident!("__column_{index}");
            Ok((name, inner))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    if function.sig.inputs.len() != expected_columns as usize + 2 {
        return Err(syn::Error::new(
            function.sig.inputs.span(),
            "batch signature must contain scalar columns, output builder, and call context",
        ));
    }
    let function_ident = &function.sig.ident;
    let wrapper = format_ident!("__radixdb_batch_{}", function_ident);
    let columns = inputs
        .iter()
        .enumerate()
        .map(|(index, (name, ty))| quote!(let #name = __input.column::<#ty>(#index)?;));
    let names = inputs.iter().map(|(name, _)| name);
    Ok(GeneratedBatch {
        items: quote! {
            unsafe extern "C" fn #wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                input: *const ::radixdb_plugin::__private::abi::RadixAbiBatchViewV1,
                output: *const ::radixdb_plugin::__private::abi::RadixAbiResultBuilderV1,
            ) -> u32 {
                unsafe {
                    ::radixdb_plugin::__private::run_batch(
                        context,
                        input,
                        output,
                        #expected_columns,
                        |__context, __input, __raw_output| {
                            #(#columns)*
                            let mut __output = ::radixdb_plugin::__private::column_builder::<#result>(__raw_output);
                            #function_ident(#(#names),*, &mut __output, __context)
                        },
                    )
                }
            }
        },
        wrapper,
    })
}

fn generate_operator_descriptor(
    function: &ItemFn,
    local_id: &str,
    options: &Options,
    package_id: [u8; 16],
    type_map: &BTreeMap<String, ([u8; 16], u32)>,
    function_ids: &BTreeMap<String, [u8; 16]>,
) -> syn::Result<TokenStream2> {
    options.reject_unknown(
        &[
            "id",
            "symbol",
            "semantic_revision",
            "function",
            "left",
            "right",
            "result",
        ],
        &[],
    )?;
    let symbol = options.required_string("symbol", function.span())?;
    let semantic_revision = options.required_u32("semantic_revision", function.span())?;
    if semantic_revision == 0 {
        return Err(syn::Error::new(
            function.span(),
            "semantic_revision must be non-zero",
        ));
    }
    let target = options.required_string("function", function.span())?;
    let function_id = function_ids.get(&target).ok_or_else(|| {
        syn::Error::new(
            function.span(),
            format!("unknown scalar function `{target}`"),
        )
    })?;
    let left = parse_type_option(options, "left", function.span())?;
    let right = parse_type_option(options, "right", function.span())?;
    let result = parse_type_option(options, "result", function.span())?;
    let left_ref = type_ref_tokens(&left, type_map)?;
    let right_ref = type_ref_tokens(&right, type_map)?;
    let result_ref = type_ref_tokens(&result, type_map)?;
    let object = bytes_tokens(&object_id(package_id, local_id));
    let function_object = bytes_tokens(function_id);
    Ok(quote! {
        ::radixdb_plugin::__private::abi::RadixAbiOperatorDescriptorV1 {
            header: ::radixdb_plugin::__private::abi::RadixAbiHeaderV1::new::<
                ::radixdb_plugin::__private::abi::RadixAbiOperatorDescriptorV1
            >(0),
            object_id: [#(#object),*],
            local_id: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#local_id.as_bytes()),
            symbol: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#symbol.as_bytes()),
            semantic_revision: #semantic_revision,
            reserved: 0,
            left: #left_ref,
            right: #right_ref,
            result: #result_ref,
            function_id: [#(#function_object),*],
        }
    })
}

fn generate_opclass_descriptor(
    function: &ItemFn,
    local_id: &str,
    options: &Options,
    package_id: [u8; 16],
    type_map: &BTreeMap<String, ([u8; 16], u32)>,
    operator_ids: &BTreeMap<(String, String, String), [u8; 16]>,
) -> syn::Result<GeneratedDescriptor> {
    options.reject_unknown(
        &[
            "id",
            "semantic_revision",
            "access_method",
            "input",
            "key",
            "key_codec_revision",
        ],
        &[],
    )?;
    let semantic_revision = options.required_u32("semantic_revision", function.span())?;
    let key_codec_revision = options.required_u32("key_codec_revision", function.span())?;
    if semantic_revision == 0 || key_codec_revision == 0 {
        return Err(syn::Error::new(
            function.span(),
            "semantic_revision and key_codec_revision must be non-zero",
        ));
    }
    let method_name = options.required_string("access_method", function.span())?;
    let (method, required_strategies): (TokenStream2, &[(u16, &str)]) = match method_name.as_str() {
        "btree" => (
            quote!(::radixdb_plugin::__private::abi::RADIX_ACCESS_METHOD_BTREE),
            &[(1, "<"), (2, "<="), (3, "="), (4, ">="), (5, ">")],
        ),
        "hash" => (
            quote!(::radixdb_plugin::__private::abi::RADIX_ACCESS_METHOD_HASH),
            &[(1, "=")],
        ),
        "bitmap" => (
            quote!(::radixdb_plugin::__private::abi::RADIX_ACCESS_METHOD_BITMAP),
            &[(1, "=")],
        ),
        "hnsw" => {
            return Err(syn::Error::new(
                function.span(),
                "external HNSW operator classes require planner support outside v1.2",
            ));
        }
        _ => {
            return Err(syn::Error::new(
                function.span(),
                "operator class access_method must be core-owned btree/hash/bitmap/hnsw",
            ));
        }
    };
    let input = parse_type_option(options, "input", function.span())?;
    let key = parse_type_option(options, "key", function.span())?;
    let input_ref = type_ref_tokens(&input, type_map)?;
    let key_ref = type_ref_tokens(&key, type_map)?;
    let function_ident = &function.sig.ident;
    let wrapper = format_ident!("__radixdb_key_{}", function_ident);
    let law_test = format_ident!("__radixdb_operator_class_laws_{}", function_ident);
    let strategy_table = format_ident!(
        "__RADIXDB_STRATEGIES_{}",
        function_ident.to_string().to_uppercase()
    );
    let input_key = quote!(#input).to_string();
    let strategies = required_strategies
        .iter()
        .map(|(slot, symbol)| {
            let key = ((*symbol).to_owned(), input_key.clone(), input_key.clone());
            let object_id = operator_ids.get(&key).ok_or_else(|| {
                syn::Error::new(
                    function.span(),
                    format!("{method_name} operator class requires `{symbol}` over `{input_key}`"),
                )
            })?;
            let object_id = bytes_tokens(object_id);
            Ok(quote! {
                ::radixdb_plugin::__private::abi::RadixAbiBindingEntryV1 {
                    slot: #slot,
                    flags: 0,
                    object_id: [#(#object_id),*],
                }
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let strategy_count = strategies.len() as u32;
    let object = bytes_tokens(&object_id(package_id, local_id));
    let fingerprint = bytes_tokens(&digest32(format!(
        "radixdb.opclass.v1\0{local_id}\0{semantic_revision}\0{key_codec_revision}\0{}",
        quote!(#function)
    )));
    let law_check = match method_name.as_str() {
        "btree" => quote!(
            ::radixdb_plugin::testing::check_btree_operator_class::<#input, #key>(#function_ident)
        ),
        "hash" => quote!(
            ::radixdb_plugin::testing::check_hash_operator_class::<#input>()
        ),
        "bitmap" => quote!(
            ::radixdb_plugin::testing::check_bitmap_operator_class::<#input, #key>(#function_ident)
        ),
        "hnsw" => quote!(Ok(())),
        _ => unreachable!("access method was validated above"),
    };
    Ok(GeneratedDescriptor {
        items: quote! {
            static #strategy_table: [
                ::radixdb_plugin::__private::abi::RadixAbiBindingEntryV1;
                #strategy_count as usize
            ] = [#(#strategies),*];

            unsafe extern "C" fn #wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                value: *const ::radixdb_plugin::__private::abi::RadixAbiValueV1,
                output: *const ::radixdb_plugin::__private::abi::RadixAbiResultBuilderV1,
            ) -> u32 {
                let callback: fn(#input) -> ::radixdb_plugin::PluginResult<#key> = #function_ident;
                unsafe {
                    ::radixdb_plugin::__private::run_key_encoder::<#input, #key>(
                        context, value, output, callback
                    )
                }
            }

            #[cfg(test)]
            #[test]
            fn #law_test() {
                #law_check.expect("operator-class law check failed");
                ::radixdb_plugin::testing::check_operator_class_strategies::<#input>(
                    &__RADIXDB_PACKAGE,
                    #local_id,
                )
                .expect("operator-class strategy law check failed");
            }
        },
        descriptor: quote! {
            ::radixdb_plugin::__private::abi::RadixAbiOperatorClassDescriptorV1 {
                header: ::radixdb_plugin::__private::abi::RadixAbiHeaderV1::new::<
                    ::radixdb_plugin::__private::abi::RadixAbiOperatorClassDescriptorV1
                >(0),
                object_id: [#(#object),*],
                local_id: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#local_id.as_bytes()),
                semantic_revision: #semantic_revision,
                access_method: #method,
                reserved_u16: 0,
                input_type: #input_ref,
                key_type: #key_ref,
                key_codec_revision: #key_codec_revision,
                strategy_count: #strategy_count,
                strategies: #strategy_table.as_ptr(),
                support_count: 0,
                reserved_u32: 0,
                supports: ::std::ptr::null(),
                fingerprint: [#(#fingerprint),*],
                encode_key: Some(#wrapper),
            }
        },
    })
}

fn generate_planner_descriptor(
    function: &ItemFn,
    local_id: &str,
    options: &Options,
    package_id: [u8; 16],
    function_ids: &BTreeMap<String, [u8; 16]>,
    opclass_ids: &BTreeMap<String, [u8; 16]>,
) -> syn::Result<GeneratedDescriptor> {
    options.reject_unknown(
        &[
            "id",
            "name",
            "semantic_revision",
            "for_function",
            "operator_class",
            "max_spans",
            "max_output_bytes",
        ],
        &["exact", "always_recheck"],
    )?;
    let _name = options.required_string("name", function.span())?;
    let semantic_revision = options.required_u32("semantic_revision", function.span())?;
    let max_spans = options.required_u32("max_spans", function.span())?;
    let max_output_bytes = options.required_u32("max_output_bytes", function.span())?;
    let policies = ["exact", "always_recheck"]
        .into_iter()
        .filter(|flag| options.flags.contains(*flag))
        .collect::<Vec<_>>();
    if policies.len() != 1 {
        return Err(syn::Error::new(
            function.span(),
            "planner support requires exactly one of exact or always_recheck",
        ));
    }
    if semantic_revision == 0 || max_spans == 0 || max_spans > 4096 || max_output_bytes == 0 {
        return Err(syn::Error::new(
            function.span(),
            "semantic_revision and planner bounds must be non-zero and max_spans <= 4096",
        ));
    }
    let target = options.required_string("for_function", function.span())?;
    let target_function = function_ids.get(&target).ok_or_else(|| {
        syn::Error::new(
            function.span(),
            format!("unknown target function `{target}`"),
        )
    })?;
    let opclass = options.required_string("operator_class", function.span())?;
    let target_opclass = opclass_ids.get(&opclass).ok_or_else(|| {
        syn::Error::new(
            function.span(),
            format!("unknown operator class `{opclass}`"),
        )
    })?;
    let recheck = if policies[0] == "exact" {
        quote!(::radixdb_plugin::__private::abi::RADIX_RECHECK_EXACT)
    } else {
        quote!(::radixdb_plugin::__private::abi::RADIX_RECHECK_ALWAYS)
    };
    let function_ident = &function.sig.ident;
    let wrapper = format_ident!("__radixdb_planner_{}", function_ident);
    let object = bytes_tokens(&object_id(package_id, local_id));
    let target_function = bytes_tokens(target_function);
    let target_opclass = bytes_tokens(target_opclass);
    let fingerprint = bytes_tokens(&digest32(format!(
        "radixdb.planner.v1\0{local_id}\0{semantic_revision}\0{max_spans}\0{max_output_bytes}\0{}",
        policies[0]
    )));
    Ok(GeneratedDescriptor {
        items: quote! {
            unsafe extern "C" fn #wrapper(
                context: *const ::radixdb_plugin::__private::abi::RadixAbiCallContextV1,
                predicate: ::radixdb_plugin::__private::abi::RadixAbiSliceV1,
                output: *const ::radixdb_plugin::__private::abi::RadixAbiResultBuilderV1,
            ) -> u32 {
                unsafe {
                    ::radixdb_plugin::__private::run_planner(
                        context,
                        predicate,
                        output,
                        |predicate, output| #function_ident(predicate, output),
                    )
                }
            }
        },
        descriptor: quote! {
            ::radixdb_plugin::__private::abi::RadixAbiPlannerSupportDescriptorV1 {
                header: ::radixdb_plugin::__private::abi::RadixAbiHeaderV1::new::<
                    ::radixdb_plugin::__private::abi::RadixAbiPlannerSupportDescriptorV1
                >(0),
                object_id: [#(#object),*],
                local_id: ::radixdb_plugin::__private::abi::RadixAbiSliceV1::from_static(#local_id.as_bytes()),
                semantic_revision: #semantic_revision,
                max_spans: #max_spans,
                max_output_bytes: #max_output_bytes,
                recheck_policy: #recheck,
                reserved_u16: 0,
                target_function_id: [#(#target_function),*],
                target_operator_class_id: [#(#target_opclass),*],
                fingerprint: [#(#fingerprint),*],
                callback: Some(#wrapper),
            }
        },
    })
}

fn function_arguments(function: &ItemFn) -> syn::Result<Vec<(Ident, Type)>> {
    function
        .sig
        .inputs
        .iter()
        .map(|argument| {
            let FnArg::Typed(argument) = argument else {
                return Err(syn::Error::new(
                    argument.span(),
                    "plugin methods are not supported",
                ));
            };
            let syn::Pat::Ident(ident) = argument.pat.as_ref() else {
                return Err(syn::Error::new(
                    argument.pat.span(),
                    "plugin arguments require simple names",
                ));
            };
            if matches!(argument.ty.as_ref(), Type::Reference(_)) {
                return Err(syn::Error::new(
                    argument.ty.span(),
                    "scalar arguments must be owned typed values",
                ));
            }
            Ok((ident.ident.clone(), (*argument.ty).clone()))
        })
        .collect()
}

fn plugin_result_type(output: &ReturnType) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new(
            output.span(),
            "plugin function must return PluginResult<T>",
        ));
    };
    generic_inner(ty, "PluginResult")
        .ok_or_else(|| syn::Error::new(ty.span(), "plugin function must return PluginResult<T>"))
}

fn generic_inner(ty: &Type, expected: &str) -> Option<Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != expected {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    })
}

fn vec_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn parse_type_option(options: &Options, name: &str, span: Span) -> syn::Result<Type> {
    let expression = options
        .values
        .get(name)
        .ok_or_else(|| syn::Error::new(span, format!("missing required `{name}`")))?;
    match expression {
        Expr::Path(path) => Ok(Type::Path(syn::TypePath {
            qself: None,
            path: path.path.clone(),
        })),
        _ => Err(syn::Error::new(
            expression.span(),
            "expected Rust type path",
        )),
    }
}

fn type_ref_tokens(
    ty: &Type,
    external: &BTreeMap<String, ([u8; 16], u32)>,
) -> syn::Result<TokenStream2> {
    let ty = generic_inner(ty, "Option").unwrap_or_else(|| ty.clone());
    let ident = terminal_type_ident(&ty)
        .ok_or_else(|| syn::Error::new(ty.span(), "unsupported plugin signature type"))?;
    let builtin = match ident.as_str() {
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" => Some(1_u16),
        "f32" | "f64" => Some(2_u16),
        "BoundedText" => Some(3_u16),
        "bool" => Some(4_u16),
        "BoundedBytes" => Some(11_u16),
        _ => None,
    };
    if let Some(tag) = builtin {
        return Ok(quote!(::radixdb_plugin::__private::abi::RadixAbiTypeRefV1::builtin(#tag)));
    }
    let (object_id, codec) = external.get(&ident).ok_or_else(|| {
        syn::Error::new(
            ty.span(),
            "signature type is neither a supported bounded built-in nor a local RadixType",
        )
    })?;
    let object = bytes_tokens(object_id);
    Ok(
        quote!(::radixdb_plugin::__private::abi::RadixAbiTypeRefV1::external(
            [#(#object),*], #codec
        )),
    )
}

fn terminal_type_ident(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn admit_local_id(ids: &mut BTreeSet<String>, value: &str, span: Span) -> syn::Result<()> {
    validate_local_id(value, span)?;
    if !ids.insert(value.to_string()) {
        return Err(syn::Error::new(
            span,
            format!("duplicate stable local id `{value}`"),
        ));
    }
    Ok(())
}

fn validate_local_id(value: &str, span: Span) -> syn::Result<()> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(syn::Error::new(
            span,
            "local id must match [a-z0-9_]{1,255}",
        ));
    }
    Ok(())
}

fn validate_package_name(value: &str, span: Span) -> syn::Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return Err(syn::Error::new(
            span,
            "package name must match [a-z0-9_-]{1,128}",
        ));
    }
    Ok(())
}

fn validate_sql_name(value: &str, span: Span) -> syn::Result<()> {
    if value.is_empty() || value.len() > 255 || value.contains('\0') {
        return Err(syn::Error::new(span, "SQL name is empty or too large"));
    }
    Ok(())
}

fn object_id(package_id: [u8; 16], local_id: &str) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"radixdb.plugin.object.v1\0");
    digest.update(package_id);
    digest.update((local_id.len() as u32).to_le_bytes());
    digest.update(local_id.as_bytes());
    digest.finalize()[..16].try_into().unwrap()
}

fn digest32(value: String) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

fn bytes_tokens<const N: usize>(bytes: &[u8; N]) -> Vec<syn::LitInt> {
    bytes
        .iter()
        .map(|byte| syn::LitInt::new(&byte.to_string(), Span::call_site()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::object_id;

    #[test]
    fn sql_rename_cannot_change_object_identity() {
        let package = [0x5a; 16];
        let before_sql_rename = object_id(package, "distance");
        let after_sql_rename = object_id(package, "distance");
        assert_eq!(before_sql_rename, after_sql_rename);
        assert_ne!(before_sql_rename, object_id(package, "distance_v2"));
        assert_ne!(before_sql_rename, object_id([0xa5; 16], "distance"));
    }
}
