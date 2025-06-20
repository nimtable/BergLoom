use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use iceberg_compact_core::compaction::CompactionBuilder;
use iceberg_compact_core::config::CompactionConfigBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure the warehouse and catalog
    let mut iceberg_configs = HashMap::new();

    let load_credentials_from_env = true;
    if load_credentials_from_env {
        iceberg_configs.insert(S3_DISABLE_CONFIG_LOAD.to_owned(), "false".to_owned());
    } else {
        iceberg_configs.insert(S3_DISABLE_CONFIG_LOAD.to_owned(), "true".to_owned());
        iceberg_configs.insert(S3_REGION.to_owned(), "us-east-1".to_owned());
        iceberg_configs.insert(S3_ENDPOINT.to_owned(), "http://localhost:9000".to_owned());
        iceberg_configs.insert(S3_ACCESS_KEY_ID.to_owned(), "xxxxxxx".to_owned());
        iceberg_configs.insert(S3_SECRET_ACCESS_KEY.to_owned(), "yyyyyyy".to_owned());
    }

    // Optional configurations for authentication
    // iceberg_configs.insert("credential".to_owned(), "your-catalog-credential".to_owned());
    // iceberg_configs.insert("token".to_owned(), "your-catalog-token".to_owned());
    // iceberg_configs.insert("oauth2-server-uri".to_owned(), "http://localhost:8080/oauth2".to_owned());
    // iceberg_configs.insert("scope".to_owned(), "your-scope".to_owned());

    let config_builder = iceberg_catalog_rest::RestCatalogConfig::builder()
        .uri("http://localhost:8080/your/catalog/uri".to_string())
        .warehouse("your-warehouse-location".to_string())
        .props(iceberg_configs);

    // 2. Create the catalog
    let catalog = Arc::new(iceberg_catalog_rest::RestCatalog::new(
        config_builder.build(),
    ));

    let namespace_ident = NamespaceIdent::new("my_namespace".into());
    let table_ident = TableIdent::new(namespace_ident, "my_table".into());

    // 3. Configure compaction settings
    let compaction_config = CompactionConfigBuilder::default().build()?;
    let compaction = CompactionBuilder::new()
        .with_catalog(catalog.clone())
        .with_table_ident(table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .with_catalog_name("my_rest_catalog".to_string())
        .build()
        .await?;

    // 4. Perform the compaction
    println!("Starting compaction for table: {}", table_ident);
    let stats = compaction.compact().await?;

    // 5. Display compaction results
    println!("Compaction completed successfully!");
    println!("  - Rewritten files: {}", stats.rewritten_files_count);
    println!("  - Added files: {}", stats.added_files_count);
    println!("  - Rewritten bytes: {}", stats.rewritten_bytes);
    println!("  - Failed files: {}", stats.failed_data_files_count);

    // optional you can check the table after compaction
    let _table = catalog.load_table(&table_ident).await?;

    Ok(())
}
