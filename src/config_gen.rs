use crate::{
    config::{ChainConfig, EventConfig, PalletConfig, ParamConfig, ScalarKind},
    pallets::is_supported_sdk_pallet,
    shared::IndexError,
};
use scale_info::{
    Field, PortableRegistry, Type, TypeDef, TypeDefPrimitive, Variant,
    form::PortableForm,
};
use std::{fs, path::Path};
use subxt::{
    Metadata, OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{RpcClient, methods::legacy::LegacyRpcMethods},
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

fn infer_scalar_kind_inner(type_id: u32, types: &PortableRegistry, depth: usize) -> Option<ScalarKind> {
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
        TypeDef::Compact(compact) => infer_scalar_kind_inner(compact.type_param.id, types, depth + 1),
        _ => None,
    }
}

pub(crate) fn infer_scalar_kind(type_id: u32, types: &PortableRegistry) -> Option<ScalarKind> {
    infer_scalar_kind_inner(type_id, types, 0)
}

pub(crate) fn infer_param(field: &Field<PortableForm>, idx: usize, types: &PortableRegistry) -> Option<ParamConfig> {
    let field_name = field.name.as_deref();
    let type_name = field.type_name.as_deref();
    let ty = types.resolve(field.ty.id)?;

    if field_name.is_some_and(is_account_field_name)
        || type_name.is_some_and(is_account_type_name)
        || type_last_segment(ty).is_some_and(is_account_type_name)
    {
        return Some(ParamConfig {
            field: field_name
                .map(str::to_owned)
                .unwrap_or_else(|| idx.to_string()),
            key: "account_id".to_owned(),
            kind: None,
        });
    }

    let kind = infer_scalar_kind(field.ty.id, types)?;

    Some(ParamConfig {
        field: field_name
            .map(str::to_owned)
            .unwrap_or_else(|| idx.to_string()),
        key: infer_key_name(field_name, type_name, ty, idx),
        kind: Some(kind),
    })
}

fn event_config(variant: &Variant<PortableForm>, types: &PortableRegistry) -> EventConfig {
    EventConfig {
        name: variant.name.clone(),
        params: variant
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| infer_param(field, idx, types))
            .collect(),
    }
}

pub(crate) fn build_chain_config(
    name: &str,
    genesis_hash: &str,
    default_url: &str,
    metadata: &Metadata,
) -> ChainConfig {
    let types = metadata.types();
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

            let events = variants.iter().map(|variant| event_config(variant, types)).collect();
            Some(PalletConfig {
                name: pallet.name().to_owned(),
                sdk: false,
                events,
            })
        })
        .collect();

    ChainConfig {
        name: name.to_owned(),
        genesis_hash: genesis_hash.to_owned(),
        default_url: default_url.to_owned(),
        versions: vec![0],
        pallets,
    }
}

pub async fn write_generated_chain_config(
    url: &str,
    output_path: &Path,
) -> Result<ChainConfig, IndexError> {
    let rpc_client = RpcClient::from_url(url).await?;
    let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client.clone()).await?;
    let rpc = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc_client);

    let runtime_version = rpc.state_get_runtime_version(None).await?;
    let metadata: Metadata = rpc.state_get_metadata(None).await?.to_frame_metadata()?.try_into()?;
    let genesis_hash = hex::encode(api.genesis_hash().as_ref());
    let chain_name = runtime_version
        .other
        .get("specName")
        .and_then(|value| value.as_str())
        .unwrap_or("custom-runtime")
        .to_owned();

    let config = build_chain_config(&chain_name, &genesis_hash, url, &metadata);
    let toml = toml::to_string_pretty(&config)?;

    if let Some(parent) = output_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, toml)?;

    Ok(config)
}
