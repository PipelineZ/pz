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
            number_from_f64(*n).map_or(JsonValue::Null, JsonValue::Number)
        }
        Some(Kind::StringValue(s)) => JsonValue::String(s.clone()),
        Some(Kind::BoolValue(b)) => JsonValue::Bool(*b),
        Some(Kind::StructValue(s)) => JsonValue::Object(struct_to_map(s)),
        Some(Kind::ListValue(ListValue { values })) => {
            JsonValue::Array(values.iter().map(value_to_json).collect())
        }
    }
}

/// protobuf's `Struct` has only `number` (an f64), so an integer connector option
/// (`max_connections: 5`) arrives here as `5.0`. `serde_json::Number::from_f64` always builds a
/// float-backed `Number` -- `as_i64()` on it returns `None` even for a whole value like `3.0` -- so a
/// connector author reading an integer option through `as_i64()` (the obvious thing to reach for) would
/// see it silently fail. Normalize an integral value within the range an f64 still represents exactly
/// (`|x| <= 2^53`) to an integer-backed `Number` instead; a fractional value, or one wider than that
/// range, stays a float, since rounding it would silently change its value or its precision.
fn number_from_f64(n: f64) -> Option<Number> {
    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_992.0; // 2^53
    if n.is_finite() && n.fract() == 0.0 && n.abs() <= MAX_SAFE_INTEGER {
        #[allow(clippy::cast_possible_truncation)]
        // guarded above: n is a whole number within i64's range
        Some(Number::from(n as i64))
    } else {
        Number::from_f64(n)
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
        // Integral: normalizes to an integer-backed Number, not the raw float every wire number
        // starts life as -- see number_normalizes_integral_values_to_i64 for the full contract.
        assert_eq!(config.0.get("count"), Some(&JsonValue::from(3i64)));
        assert_eq!(config.0.get("count").and_then(JsonValue::as_i64), Some(3));
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

    fn number_value(n: f64) -> Struct {
        let mut root = Struct::default();
        root.fields.insert(
            "k".to_string(),
            Value {
                kind: Some(Kind::NumberValue(n)),
            },
        );
        root
    }

    /// protobuf's `Struct` has only `number` (an f64), so `max_connections: 3` arrives as `3.0`.
    /// `Number::from_f64` alone would build a float-backed `Number`, and `as_i64()` on a float-backed
    /// `Number` returns `None` unconditionally -- even for a whole value -- so a connector author
    /// reading it through `as_i64()` would see it silently fail for every integer option there is.
    #[test]
    fn number_normalizes_integral_values_to_i64() {
        for (wire, expected) in [
            (5.0, 5i64),
            (-3.0, -3),
            (0.0, 0),
            (9_007_199_254_740_992.0, 9_007_199_254_740_992), // 2^53, inclusive boundary
        ] {
            let config = Config::from_struct(Some(&number_value(wire)));
            let actual = config
                .0
                .get("k")
                .and_then(JsonValue::as_i64)
                .unwrap_or_else(|| panic!("as_i64() failed for wire value {wire}"));
            assert_eq!(actual, expected, "wire value {wire}");
        }
    }

    /// A fractional value, or one wider than an f64 can represent exactly, stays a float -- rounding it
    /// would silently change its value (a non-integral) or its precision (out of range).
    #[test]
    fn number_leaves_non_integral_or_out_of_range_values_as_f64() {
        for wire in [2.5, 9_007_199_254_740_994.0] {
            // 2^53 + 2: the next f64 after the boundary is exact
            let config = Config::from_struct(Some(&number_value(wire)));
            assert_eq!(config.0.get("k").and_then(JsonValue::as_f64), Some(wire));
            assert_eq!(config.0.get("k").and_then(JsonValue::as_i64), None);
        }
    }
}
