//! Runtime projection of catalog-owned logical column types.

use radixdb_catalog::CatalogDataType;
use radixdb_core::{DataType, Error, ExternalTypeRef, Result};

use super::host::MutationHost;

type ParsedSchemaColumnType = (DataType, u16, u8, u8, Option<(ExternalTypeRef, String)>);

pub(super) fn parse_schema_column_type<H: MutationHost + ?Sized>(
    host: &H,
    type_str: &str,
) -> Result<ParsedSchemaColumnType> {
    let catalog_type: CatalogDataType = match crate::catalog::bind_catalog_type(type_str) {
        Ok(data_type) => data_type,
        Err(Error::Type(_)) if type_str.contains('.') => {
            let generation = {
                let active = host.mutation_active_transaction().lock().unwrap();
                active
                    .as_ref()
                    .map(|state| state.catalog.working_generation_shared())
            };
            let generation = match generation {
                Some(generation) => generation,
                None => host.mutation_engine().pin_catalog()?,
            };
            crate::catalog::bind_catalog_type_in_generation(type_str, generation.as_ref())?
        }
        Err(error) => return Err(error),
    };

    if let Some(type_ref) = catalog_type.external_type_ref() {
        return Ok((
            DataType::Null,
            0,
            0,
            0,
            Some((type_ref, type_str.trim().to_owned())),
        ));
    }

    let data_type = catalog_type.logical_type();
    let vector_dimensions = if data_type == DataType::Vector {
        u16::try_from(catalog_type.parameter_1())
            .map_err(|_| Error::internal("catalog vector dimensions exceed u16"))?
    } else {
        0
    };
    let (decimal_precision, decimal_scale) = if data_type == DataType::Decimal {
        (
            u8::try_from(catalog_type.parameter_1())
                .map_err(|_| Error::internal("catalog decimal precision exceeds u8"))?,
            u8::try_from(catalog_type.parameter_2())
                .map_err(|_| Error::internal("catalog decimal scale exceeds u8"))?,
        )
    } else {
        (0, 0)
    };
    Ok((
        data_type,
        vector_dimensions,
        decimal_precision,
        decimal_scale,
        None,
    ))
}
