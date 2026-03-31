#[cfg(test)]
mod config_gen_tests {
    use crate::{
        config::{ParamConfig, ScalarKind},
        config_gen::infer_param,
    };
    use scale_info::{MetaType, PortableRegistry, Registry, TypeDef, TypeInfo};

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct AccountId32([u8; 32]);

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct ParaId(u32);

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct PublishEvent {
        owner: AccountId32,
        para_id: ParaId,
        revision: u128,
        slug: String,
        published: bool,
        hash: [u8; 32],
    }

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct TupleEvent(ParaId);

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct ItemId([u8; 32]);

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct VecWrapper<T>(Vec<T>);

    #[allow(dead_code)]
    #[derive(TypeInfo)]
    struct CollectionEvent {
        parents: VecWrapper<ItemId>,
        links: VecWrapper<ItemId>,
        mentions: VecWrapper<AccountId32>,
    }

    fn event_fields<T: TypeInfo + 'static>() -> (
        PortableRegistry,
        Vec<scale_info::Field<scale_info::form::PortableForm>>,
    ) {
        let mut registry = Registry::new();
        let type_id = registry.register_type(&MetaType::new::<T>());
        let types: PortableRegistry = registry.into();
        let fields = {
            let ty = types.resolve(type_id.id).unwrap();
            let TypeDef::Composite(composite) = &ty.type_def else {
                panic!("expected composite type");
            };
            composite.fields.clone()
        };
        (types, fields)
    }

    #[test]
    fn infer_param_uses_builtin_account_id_for_account_types() {
        let (types, fields) = event_fields::<PublishEvent>();
        let param = infer_param(&fields[0], 0, &types).unwrap();
        assert_eq!(
            param,
            (
                ParamConfig {
                    field: "owner".into(),
                    key: "account_id".into(),
                    multi: false,
                },
                None,
            )
        );
    }

    #[test]
    fn infer_param_detects_scalar_wrapper_and_primitives() {
        let (types, fields) = event_fields::<PublishEvent>();
        let params: Vec<(ParamConfig, Option<ScalarKind>)> = fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| infer_param(field, idx, &types))
            .collect();

        assert!(params.contains(&(
            ParamConfig {
                field: "para_id".into(),
                key: "para_id".into(),
                multi: false,
            },
            Some(ScalarKind::U32)
        )));
        assert!(params.contains(&(
            ParamConfig {
                field: "revision".into(),
                key: "revision".into(),
                multi: false,
            },
            Some(ScalarKind::U128)
        )));
        assert!(params.contains(&(
            ParamConfig {
                field: "slug".into(),
                key: "slug".into(),
                multi: false,
            },
            Some(ScalarKind::String)
        )));
        assert!(params.contains(&(
            ParamConfig {
                field: "published".into(),
                key: "published".into(),
                multi: false,
            },
            Some(ScalarKind::Bool)
        )));
        assert!(params.contains(&(
            ParamConfig {
                field: "hash".into(),
                key: "hash".into(),
                multi: false,
            },
            Some(ScalarKind::Bytes32)
        )));
    }

    #[test]
    fn infer_param_uses_type_name_for_unnamed_fields() {
        let (types, fields) = event_fields::<TupleEvent>();
        let param = infer_param(&fields[0], 0, &types).unwrap();
        assert_eq!(
            param,
            (
                ParamConfig {
                    field: "0".into(),
                    key: "para_id".into(),
                    multi: false,
                },
                Some(ScalarKind::U32),
            )
        );
    }

    #[test]
    fn infer_param_detects_multi_item_and_account_collections() {
        let (types, fields) = event_fields::<CollectionEvent>();
        let _ = infer_param(&fields[0], 0, &types);
        let _ = infer_param(&fields[1], 1, &types);
        let _ = infer_param(&fields[2], 2, &types);
    }

    #[test]
    fn stable2603_umbrella_pallets_are_classified_as_sdk() {
        for pallet_name in [
            "System",
            "Scheduler",
            "Preimage",
            "Balances",
            "Session",
            "Grandpa",
            "Paras",
            "Registrar",
            "Hrmp",
            "Auctions",
            "Crowdloan",
            "ParaInclusion",
            "ParasDisputes",
            "Coretime",
            "Timestamp",
            "Utility",
            "TransactionPayment",
            "Sudo",
        ] {
            assert!(
                crate::pallets::is_supported_sdk_pallet(pallet_name),
                "missing sdk pallet {pallet_name}"
            );
        }
    }
}
