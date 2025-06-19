/*
 * Copyright 2025 BergLoom
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::error::Result;
use crate::test_utils::{
    docker_compose::get_rest_catalog,
    generator::{FileGenerator, FileGeneratorConfig, WriterConfig},
};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation,
    spec::{NestedField, PrimitiveType, Schema, Type},
    transaction::Transaction,
};
use std::{collections::HashMap, sync::Arc};

pub mod docker_compose;
pub mod generator;

const TABLE_NAME: &str = "t1";
const NAMESPACE_NAME: &str = "namespace";

pub fn get_test_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::new(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
                false,
            )),
            Arc::new(NestedField::new(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
                false,
            )),
        ])
        .build()?;
    Ok(schema)
}

pub async fn build_test_iceberg_table(
    schema: Option<Schema>,
    file_generator_config: Option<FileGeneratorConfig>,
) -> Result<()> {
    let catalog = get_rest_catalog().await;
    let schema = schema.unwrap_or_else(|| get_test_schema().unwrap());
    let table_creation = TableCreation::builder()
        .name(TABLE_NAME.to_string())
        .schema(schema.clone())
        .build();
    let namespace_ident = NamespaceIdent::new(NAMESPACE_NAME.to_owned());
    catalog
        .create_namespace(&namespace_ident, HashMap::default())
        .await?;
    let table = catalog
        .create_table(&namespace_ident, table_creation)
        .await?;
    let writer_config = WriterConfig::new(&table);
    let file_generator_config = file_generator_config.unwrap_or_default();
    let mut file_generator =
        FileGenerator::new(file_generator_config, Arc::new(schema), writer_config)?;
    let commit_data_files = file_generator.generate().await?;
    let mut data_files = Vec::new();
    let mut position_delete_files = Vec::new();
    let mut equality_delete_files = Vec::new();
    for data_file in commit_data_files {
        match data_file.content_type() {
            iceberg::spec::DataContentType::Data => data_files.push(data_file),
            iceberg::spec::DataContentType::PositionDeletes => {
                position_delete_files.push(data_file)
            }
            iceberg::spec::DataContentType::EqualityDeletes => {
                equality_delete_files.push(data_file)
            }
        }
    }
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn.fast_append(Some(rand::random::<i64>()), None, vec![])?;
    fast_append_action
        .add_data_files(data_files)?
        .add_data_files(position_delete_files)?;
    let table = fast_append_action.apply().await?.commit(&catalog).await?;

    let snapshot = table.metadata().current_snapshot().unwrap();
    let txn = Transaction::new(&table);
    let mut fast_append_action = txn.fast_append(Some(snapshot.snapshot_id() + 1), None, vec![])?;
    fast_append_action.add_data_files(equality_delete_files)?;
    fast_append_action.apply().await?.commit(&catalog).await?;
    Ok(())
}
