use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

use iceberg::io::FileIOBuilder;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;

use iceberg_compact_core::compaction::CompactionBuilder;
use iceberg_compact_core::config::CompactionConfigBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Set up a temporary warehouse location
    let temp_dir = TempDir::new()?;
    let warehouse_location = temp_dir.path().to_str().unwrap().to_string();

    // 2. Create file I/O and memory catalog
    let file_io = FileIOBuilder::new_fs_io().build()?;
    let catalog = Arc::new(MemoryCatalog::new(file_io, Some(warehouse_location)));

    // 3. Create namespace and table
    let namespace_ident = NamespaceIdent::new("my_namespace".into());
    catalog
        .create_namespace(&namespace_ident, HashMap::new())
        .await?;

    let table_ident = TableIdent::new(namespace_ident, "sales_data".into());

    // Define table schema
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "customer_name", Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::required(3, "amount", Type::Primitive(PrimitiveType::Double)).into(),
        ])
        .build()?;

    catalog
        .create_table(
            &table_ident.namespace,
            TableCreation::builder()
                .name(table_ident.name().into())
                .schema(schema)
                .build(),
        )
        .await?;

    // 4. Configure compaction settings
    let compaction_config = CompactionConfigBuilder::default().build()?;
    let compaction = CompactionBuilder::new()
        .with_catalog(catalog.clone())
        .with_table_ident(table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .with_catalog_name("memory_catalog".to_string())
        .build()
        .await?;

    // 5. Perform the compaction
    println!("Starting compaction for table: {}", table_ident);
    let stats = compaction.compact().await?;

    // 6. Display compaction results
    println!("Compaction completed successfully!");
    println!("  - Rewritten files: {}", stats.rewritten_files_count);
    println!("  - Added files: {}", stats.added_files_count);
    println!("  - Rewritten bytes: {}", stats.rewritten_bytes);
    println!("  - Failed files: {}", stats.failed_data_files_count);

    Ok(())
}
