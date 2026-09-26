use crate::Result;
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet};

/// Select a type without relying on descriptor file order (imports often come first).
pub fn protobuf_schema(bytes: Vec<u8>, configured: Option<&str>) -> Result<foxglove::Schema> {
    let set = FileDescriptorSet::decode(bytes.as_slice())?;
    let mut all = Vec::new();
    let mut candidates = Vec::new();
    fn names(prefix: &str, messages: &[DescriptorProto], out: &mut Vec<String>) {
        for message in messages {
            let name = if prefix.is_empty() {
                message.name().to_owned()
            } else {
                format!("{prefix}.{}", message.name())
            };
            if !message.options.as_ref().is_some_and(|o| o.map_entry()) {
                out.push(name.clone());
                names(&name, &message.nested_type, out);
            }
        }
    }
    for file in &set.file {
        for dependency in &file.dependency {
            if !set.file.iter().any(|f| f.name() == dependency) {
                return Err(format!(
                    "descriptor is missing import {dependency}; use protoc --include_imports"
                )
                .into());
            }
        }
        names(file.package(), &file.message_type, &mut all);
        // Top-level messages in files that are not imported by another file.
        if !set
            .file
            .iter()
            .any(|f| f.dependency.iter().any(|d| d == file.name()))
        {
            for message in &file.message_type {
                candidates.push(if file.package().is_empty() {
                    message.name().to_owned()
                } else {
                    format!("{}.{}", file.package(), message.name())
                });
            }
        }
    }
    let name = match configured {
        Some(name) if all.iter().any(|n| n == name.trim_start_matches('.')) => name.trim_start_matches('.').to_owned(),
        Some(name) => return Err(format!("message type {name} not found in descriptor; available: {}", all.join(", ")).into()),
        None if candidates.len() == 1 => candidates.remove(0),
        None => return Err(format!("descriptor has no unique root message; configure message_types for this topic; candidates: {}", candidates.join(", ")).into()),
    };
    Ok(foxglove::Schema::new(name, "protobuf", bytes))
}
