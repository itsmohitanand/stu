use humansize::{format_size, BINARY};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
pub use parquet::file::FOOTER_SIZE;
use parquet::schema::types::Type;

/// Parse the trailing 8-byte Parquet footer into the length (in bytes) of the
/// Thrift-encoded metadata block that precedes it.
pub fn footer_metadata_len(tail: &[u8; FOOTER_SIZE]) -> Result<usize, String> {
    ParquetMetaDataReader::decode_footer_tail(tail)
        .map(|ft| ft.metadata_length())
        .map_err(|e| e.to_string())
}

/// Build a human-readable summary (file stats + schema) from the Thrift-encoded
/// metadata block. Returns an error message string on failure so the caller can
/// display it directly as text.
///
/// ponytail: works on the footer bytes alone — the row data is never downloaded.
pub fn parquet_preview_string(metadata_bytes: &[u8]) -> String {
    match ParquetMetaDataReader::decode_metadata(metadata_bytes) {
        Ok(metadata) => build_summary(&metadata),
        Err(e) => format!("Failed to read Parquet metadata: {e}"),
    }
}

fn build_summary(metadata: &ParquetMetaData) -> String {
    let file_meta = metadata.file_metadata();
    let schema = file_meta.schema_descr();

    let fields = schema.root_schema().get_fields();

    let mut out = String::new();
    out.push_str("# File\n");
    out.push_str(&format!("Rows:       {}\n", file_meta.num_rows()));
    out.push_str(&format!("Columns:    {}\n", fields.len()));
    out.push_str(&format!("Row groups: {}\n", metadata.num_row_groups()));
    if let Some(created_by) = file_meta.created_by() {
        out.push_str(&format!("Created by: {created_by}\n"));
    }
    let compressed: i64 = (0..metadata.num_row_groups())
        .map(|i| metadata.row_group(i).compressed_size())
        .sum();
    let uncompressed: i64 = (0..metadata.num_row_groups())
        .map(|i| metadata.row_group(i).total_byte_size())
        .sum();
    out.push_str(&format!(
        "Size:       {} compressed / {} uncompressed\n",
        format_size(compressed.max(0) as u64, BINARY),
        format_size(uncompressed.max(0) as u64, BINARY),
    ));

    out.push_str("\n# Schema\n");
    let name_width = fields.iter().map(|f| f.name().len()).max().unwrap_or(0);
    for field in fields {
        out.push_str(&format!(
            "{:<width$}  {}\n",
            field.name(),
            type_label(field),
            width = name_width,
        ));
    }

    out
}

/// Human-readable datatype for a top-level schema field, e.g. `INT64`,
/// `BYTE_ARRAY (String)`, or `list<FLOAT>`.
fn type_label(t: &Type) -> String {
    if t.is_primitive() {
        let info = t.get_basic_info();
        let logical = info
            .logical_type()
            .map(|lt| format!("{lt:?}"))
            .or_else(|| {
                let ct = info.converted_type();
                (ct != ConvertedType::NONE).then(|| format!("{ct}"))
            })
            .map(|s| format!(" ({s})"))
            .unwrap_or_default();
        return format!("{}{}", t.get_physical_type(), logical);
    }

    let info = t.get_basic_info();
    if matches!(info.logical_type(), Some(LogicalType::List))
        || info.converted_type() == ConvertedType::LIST
    {
        return match list_element(t) {
            Some(elem) => format!("list<{}>", type_label(elem)),
            None => "list".to_string(),
        };
    }
    if matches!(info.logical_type(), Some(LogicalType::Map))
        || info.converted_type() == ConvertedType::MAP
    {
        return "map".to_string();
    }
    "struct".to_string()
}

/// Drill into a LIST group to reach the element type, handling both the 3-level
/// (`list` -> `element`) and legacy 2-level encodings.
fn list_element(t: &Type) -> Option<&Type> {
    let inner = t.get_fields().first()?;
    if inner.is_group() {
        inner.get_fields().first().map(|f| f.as_ref())
    } else {
        Some(inner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_len_reads_little_endian_length() {
        // 4-byte LE length + "PAR1" magic
        let tail = [0x10, 0x00, 0x00, 0x00, b'P', b'A', b'R', b'1'];
        assert_eq!(footer_metadata_len(&tail).unwrap(), 16);
    }
}
