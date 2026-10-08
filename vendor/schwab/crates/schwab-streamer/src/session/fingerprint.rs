//! Stable fingerprints used for Streamer duplicate and reorder detection.
//! 提供 Streamer 重复与乱序检测使用的稳定指纹。

use std::collections::BTreeMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::SessionRunError;

/// Hashes one raw sparse row using the Node snapshot fingerprint's value
/// equivalence while retaining only a fixed-size digest after delivery.
pub(super) fn streamer_delta_fingerprint(
    fields: &BTreeMap<String, Value>,
) -> Result<[u8; 32], SessionRunError> {
    let mut hasher = Sha256::new();
    hasher.update(b"schwab-streamer-sparse-row-v1");
    hasher.update((fields.len() as u64).to_be_bytes());

    if fields.keys().all(|field| field.is_ascii()) {
        // Rust's bytewise String ordering and JavaScript's UTF-16 ordering
        // agree for ASCII. Streamer field identifiers are ASCII, so preserve
        // the BTreeMap order directly and avoid a temporary Vec and sort.
        update_fingerprint_fields(&mut hasher, fields.iter())?;
    } else {
        // Non-ASCII keys retain the legacy JavaScript-compatible order. This
        // fallback is required because UTF-8 scalar order differs from UTF-16
        // code-unit order for some supplementary-plane characters.
        let mut entries: Vec<_> = fields.iter().collect();
        entries.sort_unstable_by(|(left, _), (right, _)| {
            left.encode_utf16().cmp(right.encode_utf16())
        });
        update_fingerprint_fields(&mut hasher, entries)?;
    }

    Ok(hasher.finalize().into())
}

fn update_fingerprint_fields<'a>(
    hasher: &mut Sha256,
    fields: impl IntoIterator<Item = (&'a String, &'a Value)>,
) -> Result<(), SessionRunError> {
    for (field, value) in fields {
        update_fingerprint_bytes(hasher, b"k", field.as_bytes());
        update_fingerprint_value(hasher, value)?;
    }
    Ok(())
}

fn update_fingerprint_bytes(hasher: &mut Sha256, marker: &[u8], bytes: &[u8]) {
    hasher.update(marker);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn update_fingerprint_value(hasher: &mut Sha256, value: &Value) -> Result<(), SessionRunError> {
    match value {
        Value::Null => {
            hasher.update(b"n");
        }
        Value::Bool(value) => {
            hasher.update(if *value { b"t" } else { b"f" });
        }
        Value::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
            let value = if value == 0.0 { 0.0 } else { value };
            hasher.update(b"d");
            hasher.update(value.to_bits().to_be_bytes());
        }
        Value::String(value) => update_fingerprint_bytes(hasher, b"s", value.as_bytes()),
        Value::Array(values) => {
            hasher.update(b"a");
            hasher.update((values.len() as u64).to_be_bytes());
            for value in values {
                update_fingerprint_value(hasher, value)?;
            }
        }
        Value::Object(values) => {
            hasher.update(b"o");
            hasher.update((values.len() as u64).to_be_bytes());
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| {
                left.encode_utf16().cmp(right.encode_utf16())
            });
            for (key, value) in entries {
                update_fingerprint_bytes(hasher, b"k", key.as_bytes());
                update_fingerprint_value(hasher, value)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::time::Instant;

    use super::*;

    #[test]
    fn ascii_field_key_fast_path_matches_legacy_digest() {
        let fields: BTreeMap<String, Value> =
            serde_json::from_str(r#"{"0":600,"2":0.5,"3":0.7,"38":1700000000000}"#)
                .expect("synthetic ASCII Streamer fields parse");

        assert!(fields.keys().all(|field| field.is_ascii()));
        assert_eq!(
            streamer_delta_fingerprint(&fields),
            reference_streamer_delta_fingerprint(&fields)
        );
    }

    #[test]
    fn unicode_field_fallback_matches_legacy_utf16_order() {
        let fields = BTreeMap::from([
            ("\u{e000}".to_owned(), Value::from(2)),
            ("\u{10000}".to_owned(), Value::from(1)),
        ]);
        let utf16_order = fields.keys().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(utf16_order, ["\u{e000}", "\u{10000}"]);

        let mut expected_utf16_order = utf16_order.clone();
        expected_utf16_order
            .sort_unstable_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
        assert_eq!(expected_utf16_order, ["\u{10000}", "\u{e000}"]);
        assert_eq!(
            streamer_delta_fingerprint(&fields),
            reference_streamer_delta_fingerprint(&fields)
        );
    }

    #[test]
    fn nested_objects_and_node_number_equivalence_match_legacy_digest() {
        let first: BTreeMap<String, Value> = serde_json::from_str(
            r#"{"quote":31,"nested":{"\uE000":2,"\uD800\uDC00":1,"a":[true,"x"]},"zero":-0.0}"#,
        )
        .expect("first nested row parses");
        let equivalent: BTreeMap<String, Value> = serde_json::from_str(
            r#"{"zero":0,"nested":{"a":[true,"x"],"\uD800\uDC00":1.0,"\uE000":2.0},"quote":31.0}"#,
        )
        .expect("equivalent nested row parses");
        let changed: BTreeMap<String, Value> = serde_json::from_str(
            r#"{"zero":0,"nested":{"a":[true,"changed"],"\uD800\uDC00":1,"\uE000":2},"quote":31}"#,
        )
        .expect("changed nested row parses");

        for fields in [&first, &equivalent, &changed] {
            assert_eq!(
                streamer_delta_fingerprint(fields),
                reference_streamer_delta_fingerprint(fields)
            );
        }
        assert_eq!(
            streamer_delta_fingerprint(&first),
            streamer_delta_fingerprint(&equivalent),
            "integer/float spellings and negative zero retain Node number equivalence"
        );
        assert_ne!(
            streamer_delta_fingerprint(&first),
            streamer_delta_fingerprint(&changed)
        );
    }

    #[test]
    #[ignore = "paired local-only release benchmark for Streamer sparse-row fingerprinting"]
    fn ignored_paired_sparse_fingerprint_benchmark() {
        const ROUNDS: usize = 1_001;
        const ITERATIONS_PER_SAMPLE: usize = 128;

        let equities = benchmark_fields(["0", "45", "46", "51", "52"]);
        let options = benchmark_fields(["0", "2", "3", "38"]);
        let sparse = (0..64).map(|index| format!("stable-{index:02}"));
        let sparse = benchmark_fields(sparse);
        let unicode = BTreeMap::from([
            (
                "\u{e000}".to_owned(),
                serde_json::json!({"quote": 31.0, "zero": -0.0}),
            ),
            ("\u{10000}".to_owned(), serde_json::json!([true, "nested"])),
        ]);

        for (profile, fields) in [
            ("equities-5", equities),
            ("options-4", options),
            ("sparse-64", sparse),
            ("unicode-fallback", unicode),
        ] {
            let expected = reference_streamer_delta_fingerprint(&fields)
                .expect("benchmark reference input is valid");
            assert_eq!(
                streamer_delta_fingerprint(&fields).expect("benchmark input is valid"),
                expected,
                "candidate output must match the legacy digest before timing"
            );

            for _ in 0..ITERATIONS_PER_SAMPLE {
                black_box(
                    reference_streamer_delta_fingerprint(black_box(&fields))
                        .expect("reference input remains valid"),
                );
                black_box(
                    streamer_delta_fingerprint(black_box(&fields))
                        .expect("candidate input remains valid"),
                );
            }

            let mut reference_samples = Vec::with_capacity(ROUNDS);
            let mut candidate_samples = Vec::with_capacity(ROUNDS);
            for round in 0..ROUNDS {
                if round % 2 == 0 {
                    reference_samples.push(measure(
                        reference_streamer_delta_fingerprint,
                        &fields,
                        ITERATIONS_PER_SAMPLE,
                    ));
                    candidate_samples.push(measure(
                        streamer_delta_fingerprint,
                        &fields,
                        ITERATIONS_PER_SAMPLE,
                    ));
                } else {
                    candidate_samples.push(measure(
                        streamer_delta_fingerprint,
                        &fields,
                        ITERATIONS_PER_SAMPLE,
                    ));
                    reference_samples.push(measure(
                        reference_streamer_delta_fingerprint,
                        &fields,
                        ITERATIONS_PER_SAMPLE,
                    ));
                }
            }

            let reference = percentile_summary(&reference_samples);
            let candidate = percentile_summary(&candidate_samples);
            let p50_candidate_vs_legacy_pct = if reference.0 == 0 {
                0.0
            } else {
                let candidate_ns = u32::try_from(candidate.0)
                    .expect("one-call benchmark duration fits u32 nanoseconds");
                let reference_ns = u32::try_from(reference.0)
                    .expect("one-call benchmark duration fits u32 nanoseconds");
                (f64::from(candidate_ns) / f64::from(reference_ns) - 1.0) * 100.0
            };
            println!(
                "SPARSE_FINGERPRINT_BENCH profile={profile} fields={} rounds={ROUNDS} iterations_per_sample={ITERATIONS_PER_SAMPLE} nearest_rank_ns_per_call legacy_p50={} legacy_p95={} legacy_p99={} candidate_p50={} candidate_p95={} candidate_p99={} p50_candidate_vs_legacy_pct={p50_candidate_vs_legacy_pct:.3}",
                fields.len(),
                reference.0,
                reference.1,
                reference.2,
                candidate.0,
                candidate.1,
                candidate.2,
            );
        }
    }

    type Fingerprint = fn(&BTreeMap<String, Value>) -> Result<[u8; 32], SessionRunError>;

    fn benchmark_fields(
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> BTreeMap<String, Value> {
        fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| (field.into(), Value::from(index as u64 + 1)))
            .collect()
    }

    fn measure(
        fingerprint: Fingerprint,
        fields: &BTreeMap<String, Value>,
        iterations: usize,
    ) -> u128 {
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(fingerprint(black_box(fields)).expect("fingerprint input remains valid"));
        }
        started.elapsed().as_nanos() / iterations as u128
    }

    fn percentile_summary(samples: &[u128]) -> (u128, u128, u128) {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let nearest_rank = |percentile: usize| {
            let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
            sorted[rank.saturating_sub(1)]
        };
        (nearest_rank(50), nearest_rank(95), nearest_rank(99))
    }

    /// Exact pre-optimization ordering and framing, retained only for differential
    /// tests and local benchmarks.
    fn reference_streamer_delta_fingerprint(
        fields: &BTreeMap<String, Value>,
    ) -> Result<[u8; 32], SessionRunError> {
        let mut hasher = Sha256::new();
        hasher.update(b"schwab-streamer-sparse-row-v1");
        hasher.update((fields.len() as u64).to_be_bytes());
        let mut entries: Vec<_> = fields.iter().collect();
        entries.sort_unstable_by(|(left, _), (right, _)| {
            left.encode_utf16().cmp(right.encode_utf16())
        });
        for (field, value) in entries {
            reference_update_bytes(&mut hasher, b"k", field.as_bytes());
            reference_update_value(&mut hasher, value)?;
        }
        Ok(hasher.finalize().into())
    }

    fn reference_update_bytes(hasher: &mut Sha256, marker: &[u8], bytes: &[u8]) {
        hasher.update(marker);
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }

    fn reference_update_value(hasher: &mut Sha256, value: &Value) -> Result<(), SessionRunError> {
        match value {
            Value::Null => hasher.update(b"n"),
            Value::Bool(value) => hasher.update(if *value { b"t" } else { b"f" }),
            Value::Number(number) => {
                let value = number
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or(SessionRunError::MarketDataCapacityExceeded)?;
                let value = if value == 0.0 { 0.0 } else { value };
                hasher.update(b"d");
                hasher.update(value.to_bits().to_be_bytes());
            }
            Value::String(value) => reference_update_bytes(hasher, b"s", value.as_bytes()),
            Value::Array(values) => {
                hasher.update(b"a");
                hasher.update((values.len() as u64).to_be_bytes());
                for value in values {
                    reference_update_value(hasher, value)?;
                }
            }
            Value::Object(values) => {
                hasher.update(b"o");
                hasher.update((values.len() as u64).to_be_bytes());
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_unstable_by(|(left, _), (right, _)| {
                    left.encode_utf16().cmp(right.encode_utf16())
                });
                for (key, value) in entries {
                    reference_update_bytes(hasher, b"k", key.as_bytes());
                    reference_update_value(hasher, value)?;
                }
            }
        }
        Ok(())
    }
}
