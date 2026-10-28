use arrow::{
    array::new_null_array,
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, path::Path, sync::Arc};

// These types survive the Parquet round trip as the same Arrow type. Other
// unsupported Arrow types are checked directly: some Parquet writers/readers
// normalize them to supported types, which must continue to succeed.
// Old -> new (TIMESTAMP/TIMESTAMPTZ cards, Oct 17): wall-clock and
// time-zone-aware microsecond Timestamps used here are now TIMESTAMP and
// TIMESTAMPTZ; TIME (microseconds) survives the round trip and stays refused.
pub fn input_types() -> Vec<DataType> {
    vec![DataType::Time64(TimeUnit::Microsecond)]
}

/// The refusal for an `input_types` column.
pub fn expected_message(ty: &DataType) -> String {
    format!("unsupported: unsupported input type: {ty}")
}

pub fn write_input(path: &Path, ty: &DataType) {
    let schema = Arc::new(Schema::new(vec![Field::new("x", ty.clone(), true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![new_null_array(ty, 3)]).unwrap();
    let mut writer = ArrowWriter::try_new(File::create_new(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
