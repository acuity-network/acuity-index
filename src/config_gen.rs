use crate::{
    config::{CustomKeyConfig, EventConfig, IndexSpec, PalletConfig, ParamConfig, ScalarKind},
    pallets::is_supported_sdk_pallet,
    shared::{IndexError, metadata_version, unsupported_metadata_error},
};
use scale_info::{
    Field, PortableRegistry, Type, TypeDef, TypeDefPrimitive, Variant, form::PortableForm,
};
use std::{collections::HashMap, fs, path::Path};
use subxt::{
    Metadata, OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{RpcClient, methods::legacy::LegacyRpcMethods, rpc_params},
};

fn is_account_field_name(name: &str) -> bool {
    matches!(
        name,
        "account"
            | "who"
            | "owner"
            | "sender"
            | "from"
            | "to"
            | "new"
            | "old"
            | "item_owner"
            | "reactor"
            | "manager"
            | "main"
            | "sub"
            | "target"
    ) || name.ends_with("_account")
}

fn is_account_type_name(type_name: &str) -> bool {
    type_name.contains("AccountId")
}

fn type_last_segment(ty: &Type<PortableForm>) -> Option<&str> {
    ty.path.segments.last().map(String::as_str)
}

fn to_snake_case(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut prev_is_lower_or_digit = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() {
                if prev_is_lower_or_digit && !out.ends_with('_') {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
                prev_is_lower_or_digit = false;
            } else {
                out.push(ch);
                prev_is_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            }
        } else if !out.ends_with('_') {
            out.push('_');
            prev_is_lower_or_digit = false;
        }
    }
    out.trim_matches('_').to_owned()
}

fn infer_key_name(
    field_name: Option<&str>,
    type_name: Option<&str>,
    ty: &Type<PortableForm>,
    idx: usize,
) -> String {
    if let Some(field_name) = field_name {
        return field_name.to_owned();
    }
    if let Some(segment) = type_last_segment(ty) {
        let name = to_snake_case(segment);
        if !name.is_empty() {
            return name;
        }
    }
    if let Some(type_name) = type_name {
        let fallback = type_name
            .split('<')
            .next()
            .unwrap_or(type_name)
            .rsplit("::")
            .next()
            .unwrap_or(type_name);
        let name = to_snake_case(fallback);
        if !name.is_empty() {
            return name;
        }
    }
    format!("field_{idx}")
}

fn is_u8_type(type_id: u32, types: &PortableRegistry) -> bool {
    matches!(
        types.resolve(type_id).map(|ty| &ty.type_def),
        Some(TypeDef::Primitive(TypeDefPrimitive::U8))
    )
}

fn collection_element_type_id(type_id: u32, types: &PortableRegistry) -> Option<u32> {
    let ty = types.resolve(type_id)?;
    match &ty.type_def {
        TypeDef::Sequence(sequence) => Some(sequence.type_param.id),
        TypeDef::Array(array) if !is_u8_type(array.type_param.id, types) || array.len != 32 => {
            Some(array.type_param.id)
        }
        TypeDef::Composite(composite) if composite.fields.len() == 1 => {
            let inner_id = composite.fields[0].ty.id;
            let inner_ty = types.resolve(inner_id)?;
            match &inner_ty.type_def {
                TypeDef::Sequence(_) => Some(inner_id),
                TypeDef::Array(array)
                    if !is_u8_type(array.type_param.id, types) || array.len != 32 =>
                {
                    Some(inner_id)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn event_collection_element_type_id(
    field: &Field<PortableForm>,
    types: &PortableRegistry,
) -> Option<u32> {
    if let Some(type_name) = field.type_name.as_deref() {
        if type_name.contains("BoundedVec<") || type_name.starts_with("Vec<") {
            let ty = types.resolve(field.ty.id)?;
            if let TypeDef::Composite(composite) = &ty.type_def {
                for inner in &composite.fields {
                    if let Some(element_id) = collection_element_type_id(inner.ty.id, types) {
                        return Some(element_id);
                    }
                }
            }
        }
    }
    collection_element_type_id(field.ty.id, types)
}

fn infer_item_key_name(
    field_name: Option<&str>,
    type_name: Option<&str>,
    ty: &Type<PortableForm>,
    idx: usize,
) -> String {
    if type_last_segment(ty) == Some("ItemId")
        || type_name.is_some_and(|name| name.contains("ItemId"))
    {
        return "item_id".to_owned();
    }
    let inferred = infer_key_name(field_name, type_name, ty, idx);
    if inferred.ends_with("item_id") || inferred == "item_id" {
        "item_id".to_owned()
    } else {
        inferred
    }
}

fn infer_scalar_kind_inner(
    type_id: u32,
    types: &PortableRegistry,
    depth: usize,
) -> Option<ScalarKind> {
    if depth > 8 {
        return None;
    }

    let ty = types.resolve(type_id)?;
    match &ty.type_def {
        TypeDef::Primitive(primitive) => match primitive {
            TypeDefPrimitive::Bool => Some(ScalarKind::Bool),
            TypeDefPrimitive::Str => Some(ScalarKind::String),
            TypeDefPrimitive::U32 => Some(ScalarKind::U32),
            TypeDefPrimitive::U64 => Some(ScalarKind::U64),
            TypeDefPrimitive::U128 => Some(ScalarKind::U128),
            _ => None,
        },
        TypeDef::Array(array) if array.len == 32 && is_u8_type(array.type_param.id, types) => {
            Some(ScalarKind::Bytes32)
        }
        TypeDef::Composite(composite) if composite.fields.len() == 1 => {
            infer_scalar_kind_inner(composite.fields[0].ty.id, types, depth + 1)
        }
        TypeDef::Tuple(tuple) if tuple.fields.len() == 1 => {
            infer_scalar_kind_inner(tuple.fields[0].id, types, depth + 1)
        }
        TypeDef::Compact(compact) => {
            infer_scalar_kind_inner(compact.type_param.id, types, depth + 1)
        }
        _ => None,
    }
}

pub(crate) fn infer_scalar_kind(type_id: u32, types: &PortableRegistry) -> Option<ScalarKind> {
    infer_scalar_kind_inner(type_id, types, 0)
}

pub(crate) fn infer_param(
    field: &Field<PortableForm>,
    idx: usize,
    types: &PortableRegistry,
) -> Option<(ParamConfig, Option<ScalarKind>)> {
    let field_name = field.name.as_deref();
    let type_name = field.type_name.as_deref();
    let ty = types.resolve(field.ty.id)?;
    let field_ref = field_name
        .map(str::to_owned)
        .unwrap_or_else(|| idx.to_string());

    if let Some(element_type_id) = event_collection_element_type_id(field, types) {
        let element_ty = types.resolve(element_type_id)?;
        let element_type_name = field
            .type_name
            .as_deref()
            .and_then(|name| name.split('<').nth(1))
            .and_then(|name| name.split(',').next())
            .map(str::trim);
        if field_name.is_some_and(is_account_field_name)
            || element_type_name.is_some_and(is_account_type_name)
            || type_last_segment(element_ty).is_some_and(is_account_type_name)
        {
            return Some((
                ParamConfig {
                    field: Some(field_ref),
                    fields: vec![],
                    key: "account_id".to_owned(),
                    multi: true,
                },
                None,
            ));
        }

        let kind = infer_scalar_kind(element_type_id, types)?;
        return Some((
            ParamConfig {
                field: Some(field_ref),
                fields: vec![],
                key: infer_item_key_name(field_name, type_name, element_ty, idx),
                multi: true,
            },
            Some(kind),
        ));
    }

    if field_name.is_some_and(is_account_field_name)
        || type_name.is_some_and(is_account_type_name)
        || type_last_segment(ty).is_some_and(is_account_type_name)
    {
        return Some((
            ParamConfig {
                field: Some(field_ref),
                fields: vec![],
                key: "account_id".to_owned(),
                multi: false,
            },
            None,
        ));
    }

    let kind = infer_scalar_kind(field.ty.id, types)?;

    Some((
        ParamConfig {
            field: Some(field_ref),
            fields: vec![],
            key: infer_key_name(field_name, type_name, ty, idx),
            multi: false,
        },
        Some(kind),
    ))
}

fn event_config(
    variant: &Variant<PortableForm>,
    types: &PortableRegistry,
    custom_keys: &mut HashMap<String, CustomKeyConfig>,
) -> EventConfig {
    EventConfig {
        name: variant.name.clone(),
        params: variant
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| infer_param(field, idx, types))
            .map(|(param, kind)| {
                if let Some(kind) = kind {
                    custom_keys.insert(param.key.clone(), CustomKeyConfig::Scalar(kind));
                }
                param
            })
            .collect(),
    }
}

pub(crate) fn build_index_spec(
    name: &str,
    genesis_hash: &str,
    default_url: &str,
    metadata: &Metadata,
) -> IndexSpec {
    let types = metadata.types();
    let mut custom_keys = HashMap::new();
    let pallets = metadata
        .pallets()
        .filter_map(|pallet| {
            let variants = pallet.event_variants()?;
            if is_supported_sdk_pallet(pallet.name()) {
                return Some(PalletConfig {
                    name: pallet.name().to_owned(),
                    sdk: true,
                    events: vec![],
                });
            }

            let events = variants
                .iter()
                .map(|variant| event_config(variant, types, &mut custom_keys))
                .collect();
            Some(PalletConfig {
                name: pallet.name().to_owned(),
                sdk: false,
                events,
            })
        })
        .collect();

    IndexSpec {
        name: name.to_owned(),
        genesis_hash: genesis_hash.to_owned(),
        default_url: default_url.to_owned(),
        versions: vec![0],
        index_variant: false,
        store_events: false,
        custom_keys,
        pallets,
    }
}

fn inline_string(value: &str) -> Result<String, IndexError> {
    Ok(toml::Value::String(value.to_owned()).to_string())
}

fn scalar_kind_name(kind: &ScalarKind) -> &'static str {
    match kind {
        ScalarKind::Bytes32 => "bytes32",
        ScalarKind::U32 => "u32",
        ScalarKind::U64 => "u64",
        ScalarKind::U128 => "u128",
        ScalarKind::String => "string",
        ScalarKind::Bool => "bool",
    }
}

fn inline_params(params: &[ParamConfig]) -> Result<String, IndexError> {
    if params.is_empty() {
        return Ok("[]".to_owned());
    }

    let mut out = String::from("[");
    for param in params {
        out.push_str("\n    { ");
        if let Some(field) = &param.field {
            out.push_str("field = ");
            out.push_str(&inline_string(field)?);
        } else {
            out.push_str("fields = [");
            for (index, field) in param.fields.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&inline_string(field)?);
            }
            out.push(']');
        }
        out.push_str(", key = ");
        out.push_str(&inline_string(&param.key)?);
        if param.multi {
            out.push_str(", multi = true");
        }
        out.push_str(" },");
    }
    out.push_str("\n  ]");
    Ok(out)
}

fn inline_events(events: &[EventConfig]) -> Result<String, IndexError> {
    if events.is_empty() {
        return Ok("[]".to_owned());
    }

    let mut out = String::from("[");
    for event in events {
        out.push_str("\n  { name = ");
        out.push_str(&inline_string(&event.name)?);
        if event.params.is_empty() {
            out.push_str(" },");
        } else {
            out.push_str(", params = ");
            out.push_str(&inline_params(&event.params)?);
            out.push_str(" },");
        }
    }
    out.push_str("\n]");
    Ok(out)
}

pub(crate) fn render_index_spec_toml(spec: &IndexSpec) -> Result<String, IndexError> {
    let mut out = String::new();
    out.push_str("name = ");
    out.push_str(&inline_string(&spec.name)?);
    out.push('\n');
    out.push_str("genesis_hash = ");
    out.push_str(&inline_string(&spec.genesis_hash)?);
    out.push('\n');
    out.push_str("default_url = ");
    out.push_str(&inline_string(&spec.default_url)?);
    out.push('\n');
    out.push_str("versions = [");
    for (index, version) in spec.versions.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&version.to_string());
    }
    out.push(']');
    if spec.index_variant {
        out.push_str("\nindex_variant = true");
    }
    if spec.store_events {
        out.push_str("\nstore_events = true");
    }
    out.push_str("\n\n");

    if !spec.custom_keys.is_empty() {
        out.push_str("[custom_keys]\n");
        let mut custom_keys: Vec<_> = spec.custom_keys.iter().collect();
        custom_keys.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (name, kind) in custom_keys {
            out.push_str(name);
            out.push_str(" = ");
            match kind {
                CustomKeyConfig::Scalar(kind) => {
                    out.push_str(&inline_string(scalar_kind_name(kind))?);
                }
                CustomKeyConfig::Composite(cfg) => {
                    out.push_str("{ kind = ");
                    out.push_str(&inline_string("composite")?);
                    out.push_str(", fields = [");
                    for (index, field_kind) in cfg.fields.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        out.push_str(&inline_string(scalar_kind_name(field_kind))?);
                    }
                    out.push_str("] }");
                }
            }
            out.push('\n');
        }
        out.push('\n');
    }

    for (index, pallet) in spec.pallets.iter().enumerate() {
        out.push_str("[[pallets]]\n");
        out.push_str("name = ");
        out.push_str(&inline_string(&pallet.name)?);
        out.push('\n');
        if pallet.sdk {
            out.push_str("sdk = true\n");
        }
        if !pallet.events.is_empty() {
            out.push_str("events = ");
            out.push_str(&inline_events(&pallet.events)?);
            out.push('\n');
        }
        if index + 1 != spec.pallets.len() {
            out.push('\n');
        }
    }

    Ok(out)
}

pub async fn write_generated_index_spec(
    url: &str,
    output_path: &Path,
) -> Result<IndexSpec, IndexError> {
    let rpc_client = RpcClient::from_url(url).await?;
    let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client.clone()).await?;
    let rpc = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc_client.clone());

    let runtime_version = rpc.state_get_runtime_version(None).await?;
    let metadata_hex: String = rpc_client
        .request(
            "state_getMetadata",
            rpc_params![Option::<subxt::utils::H256>::None],
        )
        .await?;
    let metadata_bytes = hex::decode(metadata_hex.strip_prefix("0x").unwrap_or(&metadata_hex))?;
    let spec_name = runtime_version
        .other
        .get("specName")
        .and_then(|value| value.as_str())
        .unwrap_or("custom-runtime");
    let spec_version = u64::from(runtime_version.spec_version);
    if let Some(version) = metadata_version(&metadata_bytes) && version < 14 {
        return Err(unsupported_metadata_error(version, spec_name, spec_version));
    }
    let metadata: Metadata = rpc.state_get_metadata(None).await?.to_frame_metadata()?.try_into()?;
    let genesis_hash = hex::encode(api.genesis_hash().as_ref());
    let chain_name = spec_name.to_owned();

    let spec = build_index_spec(&chain_name, &genesis_hash, url, &metadata);
    let toml = render_index_spec_toml(&spec)?;

    if let Some(parent) = output_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, toml)?;

    Ok(spec)
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use scale_info::{MetaType, Registry, TypeInfo};

    #[derive(TypeInfo)]
    struct WrapperU64(u64);

    #[derive(TypeInfo)]
    struct WrapperBool(bool);

    #[derive(TypeInfo)]
    struct TupleWrapper(WrapperU64);

    #[derive(TypeInfo)]
    struct Unsupported([u8; 16]);

    #[derive(TypeInfo)]
    struct CompactWrapper(subxt::ext::codec::Compact<u32>);

    #[derive(TypeInfo)]
    struct UnsupportedEvent {
        flag: [u8; 16],
    }

    #[derive(TypeInfo)]
    enum DemoEvent {
        Published { owner: AccountId32, revision: u64 },
    }

    #[derive(TypeInfo)]
    struct AccountId32([u8; 32]);

    #[derive(TypeInfo)]
    struct VecWrapper<T>(Vec<T>);

    #[derive(TypeInfo)]
    struct MultiEvent {
        parents: VecWrapper<ItemId32>,
        mentions: VecWrapper<AccountId32>,
    }

    #[derive(TypeInfo)]
    struct ItemId32([u8; 32]);

    fn registry() -> PortableRegistry {
        let mut registry = Registry::new();
        registry.register_type(&MetaType::new::<WrapperU64>());
        registry.register_type(&MetaType::new::<WrapperBool>());
        registry.register_type(&MetaType::new::<TupleWrapper>());
        registry.register_type(&MetaType::new::<Unsupported>());
        registry.register_type(&MetaType::new::<CompactWrapper>());
        registry.register_type(&MetaType::new::<DemoEvent>());
        registry.into()
    }

    #[test]
    fn snake_case_handles_boundaries_and_symbols() {
        assert_eq!(to_snake_case("ParaId"), "para_id");
        assert_eq!(to_snake_case("My-Value Name"), "my_value_name");
        assert_eq!(to_snake_case("HTTPServer2"), "httpserver2");
        assert_eq!(to_snake_case("***"), "");
    }

    #[test]
    fn infer_key_name_prefers_field_then_type_segment_then_type_name_then_index() {
        let types = registry();
        let wrapper_ty = types
            .types
            .iter()
            .find(|ty| {
                ty.ty
                    .path
                    .segments
                    .last()
                    .is_some_and(|segment| segment == "WrapperU64")
            })
            .map(|ty| &ty.ty)
            .unwrap();

        assert_eq!(infer_key_name(Some("owner"), None, wrapper_ty, 7), "owner");
        assert_eq!(infer_key_name(None, None, wrapper_ty, 7), "wrapper_u64");

        let no_segment_ty = Type {
            path: Default::default(),
            type_params: vec![],
            type_def: TypeDef::Primitive(TypeDefPrimitive::U32),
            docs: vec![],
        };
        assert_eq!(
            infer_key_name(None, Some("foo::BarBaz"), &no_segment_ty, 7),
            "bar_baz"
        );
        assert_eq!(infer_key_name(None, None, &no_segment_ty, 7), "field_7");
    }

    #[test]
    fn infer_scalar_kind_supports_wrappers_and_rejects_unsupported_types() {
        let mut registry = Registry::new();
        let u64_id = registry.register_type(&MetaType::new::<WrapperU64>()).id;
        let bool_id = registry.register_type(&MetaType::new::<WrapperBool>()).id;
        let tuple_id = registry.register_type(&MetaType::new::<TupleWrapper>()).id;
        let unsupported_id = registry.register_type(&MetaType::new::<Unsupported>()).id;
        let plain_tuple_id = registry.register_type(&MetaType::new::<(u32,)>()).id;
        let compact_id = registry
            .register_type(&MetaType::new::<CompactWrapper>())
            .id;
        let u16_id = registry.register_type(&MetaType::new::<u16>()).id;
        let types: PortableRegistry = registry.into();

        assert_eq!(infer_scalar_kind(u64_id, &types), Some(ScalarKind::U64));
        assert_eq!(infer_scalar_kind(bool_id, &types), Some(ScalarKind::Bool));
        assert_eq!(infer_scalar_kind(tuple_id, &types), Some(ScalarKind::U64));
        assert_eq!(
            infer_scalar_kind(plain_tuple_id, &types),
            Some(ScalarKind::U32)
        );
        assert_eq!(infer_scalar_kind(compact_id, &types), Some(ScalarKind::U32));
        assert_eq!(infer_scalar_kind(unsupported_id, &types), None);
        assert_eq!(infer_scalar_kind(u16_id, &types), None);
        assert_eq!(infer_scalar_kind(999_999, &types), None);
        assert_eq!(infer_scalar_kind_inner(u64_id, &types, 9), None);
    }

    #[test]
    fn event_config_collects_inferred_params() {
        let mut registry = Registry::new();
        let event_id = registry.register_type(&MetaType::new::<DemoEvent>()).id;
        let types: PortableRegistry = registry.into();
        let ty = types.resolve(event_id).unwrap();
        let TypeDef::Variant(variant_def) = &ty.type_def else {
            panic!("expected variant type");
        };
        let variant = &variant_def.variants[0];

        let mut custom_keys = HashMap::new();
        let config = event_config(variant, &types, &mut custom_keys);

        assert_eq!(config.name, "Published");
        assert_eq!(config.params.len(), 2);
        assert_eq!(config.params[0].key, "account_id");
        assert_eq!(config.params[1].key, "revision");
        assert_eq!(
            custom_keys.get("revision"),
            Some(&CustomKeyConfig::Scalar(ScalarKind::U64))
        );
    }

    #[test]
    fn infer_key_name_falls_back_to_index_when_names_are_empty() {
        let ty = Type {
            path: Default::default(),
            type_params: vec![],
            type_def: TypeDef::Primitive(TypeDefPrimitive::U32),
            docs: vec![],
        };

        assert_eq!(infer_key_name(None, Some("***"), &ty, 3), "field_3");
    }

    #[test]
    fn infer_param_returns_none_for_unknown_type_and_unnamed_account_field() {
        let mut registry = Registry::new();
        let event_id = registry.register_type(&MetaType::new::<DemoEvent>()).id;
        let types: PortableRegistry = registry.into();
        let ty = types.resolve(event_id).unwrap();
        let TypeDef::Variant(variant_def) = &ty.type_def else {
            panic!("expected variant type");
        };

        let mut owner_field = variant_def.variants[0].fields[0].clone();
        owner_field.name = None;
        assert_eq!(
            infer_param(&owner_field, 0, &types),
            Some((
                ParamConfig {
                    field: Some("0".into()),
                    fields: vec![],
                    key: "account_id".into(),
                    multi: false,
                },
                None,
            ))
        );

        let mut unsupported_registry = Registry::new();
        let event_id = unsupported_registry
            .register_type(&MetaType::new::<UnsupportedEvent>())
            .id;
        let unsupported_types: PortableRegistry = unsupported_registry.into();
        let unsupported_ty = unsupported_types.resolve(event_id).unwrap();
        let TypeDef::Composite(composite) = &unsupported_ty.type_def else {
            panic!("expected composite type");
        };
        assert_eq!(
            infer_param(&composite.fields[0], 0, &unsupported_types),
            None
        );
    }

    #[test]
    fn infer_param_detects_multi_value_item_and_account_collections() {
        let mut registry = Registry::new();
        let type_id = registry.register_type(&MetaType::new::<MultiEvent>()).id;
        let types: PortableRegistry = registry.into();
        let ty = types.resolve(type_id).unwrap();
        let TypeDef::Composite(composite) = &ty.type_def else {
            panic!("expected composite type");
        };

        let _ = infer_param(&composite.fields[0], 0, &types);
        let _ = infer_param(&composite.fields[1], 1, &types);
    }

    #[test]
    fn render_index_spec_toml_inlines_events_and_params() {
        let spec = IndexSpec {
            name: "acuity-runtime".into(),
            genesis_hash: "00".repeat(32),
            default_url: "ws://127.0.0.1:9944".into(),
            versions: vec![0],
            index_variant: false,
            store_events: false,
            custom_keys: HashMap::from([
                ("item_id".into(), CustomKeyConfig::Scalar(ScalarKind::Bytes32)),
                (
                    "revision_id".into(),
                    CustomKeyConfig::Scalar(ScalarKind::U32),
                ),
            ]),
            pallets: vec![PalletConfig {
                name: "Content".into(),
                sdk: false,
                events: vec![
                    EventConfig {
                        name: "PublishItem".into(),
                        params: vec![
                            ParamConfig {
                                field: Some("item_id".into()),
                                fields: vec![],
                                key: "item_id".into(),
                                multi: false,
                            },
                            ParamConfig {
                                field: Some("owner".into()),
                                fields: vec![],
                                key: "account_id".into(),
                                multi: false,
                            },
                        ],
                    },
                    EventConfig {
                        name: "PublishRevision".into(),
                        params: vec![
                            ParamConfig {
                                field: Some("item_id".into()),
                                fields: vec![],
                                key: "item_id".into(),
                                multi: false,
                            },
                            ParamConfig {
                                field: Some("owner".into()),
                                fields: vec![],
                                key: "account_id".into(),
                                multi: false,
                            },
                            ParamConfig {
                                field: Some("revision_id".into()),
                                fields: vec![],
                                key: "revision_id".into(),
                                multi: false,
                            },
                        ],
                    },
                ],
            }],
        };

        let toml = render_index_spec_toml(&spec).unwrap();

        assert!(toml.contains("[custom_keys]\nitem_id = \"bytes32\"\nrevision_id = \"u32\"\n"));
        assert!(toml.contains("[[pallets]]\nname = \"Content\"\nevents = ["));
        assert!(toml.contains("{ name = \"PublishItem\", params = ["));
        assert!(toml.contains("{ field = \"item_id\", key = \"item_id\" }"));
        assert!(toml.contains("{ field = \"owner\", key = \"account_id\" }"));
        assert!(toml.contains("{ name = \"PublishRevision\", params = ["));
    }
}
