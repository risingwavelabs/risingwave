use anyhow::{anyhow, bail};
use darling::FromMeta;
use syn::{Attribute, Meta, NestedMeta, Path};

use crate::internal::symbol::PB_CONVERT;

#[derive(FromMeta)]
pub(crate) struct ContainerAttrs {
    pb_type: Path,
}

/// Attributes of field.
#[derive(FromMeta)]
pub(crate) struct FieldAttrs {
    #[darling(default)]
    pb_field: Option<syn::Ident>,
    #[darling(default)]
    skip: bool,
}

/// Attributes of enum variant.
#[derive(FromMeta)]
pub(crate) struct VariantAttrs {
    #[darling(default)]
    pb_variant: Option<syn::Ident>,
}

impl ContainerAttrs {
    pub(crate) fn from_ast(item: &syn::DeriveInput) -> anyhow::Result<Self> {
        let nested_meta_list = item
            .attrs
            .iter()
            .flat_map(collect_nested_meta)
            .flatten()
            .collect::<Vec<NestedMeta>>();

        Self::from_list(&nested_meta_list).map_err(|e| anyhow!("Failed to parse meta data: {}", e))
    }

    pub(crate) fn pb_type(&self) -> &Path {
        &self.pb_type
    }
}

impl FieldAttrs {
    pub(crate) fn from_ast(ast: &syn::Field) -> anyhow::Result<FieldAttrs> {
        let nested_meta_list = ast
            .attrs
            .iter()
            .flat_map(collect_nested_meta)
            .flatten()
            .collect::<Vec<NestedMeta>>();

        Self::from_list(&nested_meta_list).map_err(|e| anyhow!("Failed to parse meta data: {}", e))
    }

    pub(crate) fn pb_field(&self) -> Option<&syn::Ident> {
        self.pb_field.as_ref()
    }

    pub(crate) fn skipped(&self) -> bool {
        self.skip
    }
}

impl VariantAttrs {
    pub(crate) fn from_ast(ast: &syn::Variant) -> anyhow::Result<Self> {
        let nested_meta_list = ast
            .attrs
            .iter()
            .flat_map(collect_nested_meta)
            .flatten()
            .collect::<Vec<NestedMeta>>();

        Self::from_list(&nested_meta_list).map_err(|e| anyhow!("Failed to parse meta data: {}", e))
    }

    pub(crate) fn pb_variant(&self) -> Option<&syn::Ident> {
        self.pb_variant.as_ref()
    }
}

fn collect_nested_meta(attr: &Attribute) -> anyhow::Result<Vec<NestedMeta>> {
    if attr.path != PB_CONVERT {
        return Ok(Vec::default());
    }

    match attr.parse_meta()? {
        Meta::List(meta_list) => Ok(meta_list.nested.into_iter().collect()),
        _ => bail!("Expecting #[pb_convert(...)]"),
    }
}
