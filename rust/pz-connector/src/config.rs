use prost_types::{value::Kind, ListValue, Struct, Value};
use serde_json::{Map, Number, Value as JsonValue};

/// One connector instance's configuration, as delivered by the `Configure` RPC:
/// `google.protobuf.Struct` is the only shape configuration ever arrives in (a string/number/bool/null/
/// list/nested-map tree), converted here into the JSON shape every Rust connector author already knows
/// how to read.
///
/// <div class="warning">
///
/// **Field order is not guaranteed.** Some connectors bind a `columns:` contract to a CSV/positional
/// schema by the ORDER keys appear in a nested options map. `Struct`'s wire format has no map-ordering
/// guarantee (proto3 maps are explicitly unordered), and `prost_types::Struct` decodes `fields` into a
/// `BTreeMap` -- so by the time this type is built, any insertion order the sending host used is already
/// gone; iteration here is key-sorted, not wire order. This crate uses `serde_json::Map` with the
/// `preserve_order` feature specifically so it does not destroy order a SECOND time on top of that (a
/// plain `serde_json::Map` defaults to yet another `BTreeMap`), but it cannot recover order that never
/// survived the protobuf decode. Tracked as a protocol-level hazard, not something a connector SDK can
/// paper over on its own.
///
/// </div>
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Config(pub Map<String, JsonValue>);

impl Config {
    pub(crate) fn from_struct(value: Option<&Struct>) -> Self {
        Config(match value {
            Some(s) => struct_to_map(s),
            None => Map::new(),
        })
    }
}

fn struct_to_map(value: &Struct) -> Map<String, JsonValue> {
    value
        .fields
        .iter()
        .map(|(key, item)| (key.clone(), value_to_json(item)))
        .collect()
}

fn value_to_json(value: &Value) -> JsonValue {
    match &value.kind {
        None | Some(Kind::NullValue(_)) => JsonValue::Null,
        Some(Kind::NumberValue(n)) => {
            Number::from_f64(*n).map_or(JsonValue::Null, JsonValue::Number)
        }
        Some(Kind::StringValue(s)) => JsonValue::String(s.clone()),
        Some(Kind::BoolValue(b)) => JsonValue::Bool(*b),
        Some(Kind::StructValue(s)) => JsonValue::Object(struct_to_map(s)),
        Some(Kind::ListValue(ListValue { values })) => {
            JsonValue::Array(values.iter().map(value_to_json).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_every_struct_value_kind() {
        let mut nested = Struct::default();
        nested.fields.insert(
            "inner".to_string(),
            Value {
                kind: Some(Kind::BoolValue(true)),
            },
        );

        let mut root = Struct::default();
        root.fields.insert(
            "name".to_string(),
            Value {
                kind: Some(Kind::StringValue("csv".to_string())),
            },
        );
        root.fields.insert(
            "count".to_string(),
            Value {
                kind: Some(Kind::NumberValue(3.0)),
            },
        );
        root.fields.insert(
            "missing".to_string(),
            Value {
                kind: Some(Kind::NullValue(0)),
            },
        );
        root.fields.insert(
            "tags".to_string(),
            Value {
                kind: Some(Kind::ListValue(ListValue {
                    values: vec![
                        Value {
                            kind: Some(Kind::StringValue("a".to_string())),
                        },
                        Value {
                            kind: Some(Kind::StringValue("b".to_string())),
                        },
                    ],
                })),
            },
        );
        root.fields.insert(
            "nested".to_string(),
            Value {
                kind: Some(Kind::StructValue(nested)),
            },
        );

        let config = Config::from_struct(Some(&root));

        assert_eq!(
            config.0.get("name"),
            Some(&JsonValue::String("csv".to_string()))
        );
        assert_eq!(config.0.get("count"), Some(&JsonValue::from(3.0)));
        assert_eq!(config.0.get("missing"), Some(&JsonValue::Null));
        assert_eq!(
            config.0.get("tags"),
            Some(&JsonValue::Array(vec![
                JsonValue::String("a".to_string()),
                JsonValue::String("b".to_string())
            ]))
        );
        let nested_obj = config
            .0
            .get("nested")
            .and_then(JsonValue::as_object)
            .expect("nested object");
        assert_eq!(nested_obj.get("inner"), Some(&JsonValue::Bool(true)));
    }

    #[test]
    fn absent_config_is_empty_map() {
        assert_eq!(Config::from_struct(None).0.len(), 0);
    }
}
