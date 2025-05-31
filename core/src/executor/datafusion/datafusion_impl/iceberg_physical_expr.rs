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

use core::{any::Any, hash::Hash};
use std::sync::Arc;

use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Schema},
    },
    common::internal_err,
    logical_expr::ColumnarValue,
    physical_plan::PhysicalExpr,
};
use iceberg::{
    spec::Transform,
    transform::{BoxedTransformFunction, create_transform_function},
};
use iceberg_datafusion::to_datafusion_error;
use std::fmt::Debug;

pub struct IcebergTransformPhysicalExpr {
    name: String,
    index: usize,
    transform: BoxedTransformFunction,
}

impl Debug for IcebergTransformPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTransformPhysicalExpr")
            .field("name", &self.name)
            .field("index", &self.index)
            .finish()
    }
}

impl std::fmt::Display for IcebergTransformPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("IcebergPhysicalExpr")
            .field("name", &self.name)
            .field("index", &self.index)
            .finish()
    }
}

impl Hash for IcebergTransformPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.index.hash(state);
    }
}

impl PartialEq for IcebergTransformPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.index == other.index
    }
}

impl Eq for IcebergTransformPhysicalExpr {}

impl PhysicalExpr for IcebergTransformPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::error::Result<DataType> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::error::Result<bool> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::error::Result<ColumnarValue> {
        self.bounds_check(batch.schema().as_ref())?;
        let res_array = self
            .transform
            .transform(batch.column(self.index).clone())
            .unwrap();
        Ok(ColumnarValue::Array(res_array.clone()))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

impl IcebergTransformPhysicalExpr {
    fn bounds_check(&self, input_schema: &Schema) -> datafusion::error::Result<()> {
        if self.index < input_schema.fields.len() {
            Ok(())
        } else {
            internal_err!(
                "PhysicalExpr Column references column '{}' at index {} (zero-based) but input schema only has {} columns: {:?}",
                self.name,
                self.index,
                input_schema.fields.len(),
                input_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )
        }
    }

    pub fn new(
        name: String,
        index: usize,
        transform: Transform,
    ) -> datafusion::error::Result<Self> {
        let transform =
            create_transform_function(&transform).map_err(|err| to_datafusion_error(err))?;
        Ok(Self {
            name,
            index,
            transform,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use iceberg::spec::Transform;
    use std::sync::Arc;

    #[test]
    fn test_evaluate_identity() {
        let schema = Schema::new(vec![Field::new("v1", DataType::Int32, false)]);
        let v1 = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(v1.clone())]).unwrap();
        let expr = IcebergTransformPhysicalExpr::new("v1".to_string(), 0, Transform::Identity).unwrap();
        let result = expr.evaluate(&batch).unwrap();
        match result {
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(arr.values(), v1.values());
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_evaluate_bucket() {
        let schema = Schema::new(vec![Field::new("v1", DataType::Int32, false)]);
        let v1 = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(v1.clone())]).unwrap();
        let expr = IcebergTransformPhysicalExpr::new("v1".to_string(), 0, Transform::Bucket(4)).unwrap();
        let result = expr.evaluate(&batch).unwrap();
        match result {
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(
                    arr.values(),
                    Int32Array::from(vec![0, 0, 3, 2, 3, 1, 3, 3, 3]).values()
                );
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_evaluate_truncate() {
        let schema = Schema::new(vec![Field::new("v1", DataType::Int32, false)]);
        let v1 = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(v1.clone())]).unwrap();
        let expr = IcebergTransformPhysicalExpr::new("v1".to_string(), 0, Transform::Truncate(3)).unwrap();
        let result = expr.evaluate(&batch).unwrap();
        match result {
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(
                    arr.values(),
                    Int32Array::from(vec![0, 0, 3, 3, 3, 6, 6, 6, 9]).values()
                );
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_evaluate_void() {
        let schema = Schema::new(vec![Field::new("v1", DataType::Int32, false)]);
        let v1 = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(v1.clone())]).unwrap();
        let expr = IcebergTransformPhysicalExpr::new("v1".to_string(), 0, Transform::Void).unwrap();
        let result = expr.evaluate(&batch).unwrap();
        match result {
            datafusion::logical_expr::ColumnarValue::Array(array) => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(
                    arr.values(),
                    Int32Array::from(vec![0, 0, 0, 0, 0, 0, 0, 0, 0]).values()
                );
            }
            _ => panic!("Expected array result"),
        }
    }
}
