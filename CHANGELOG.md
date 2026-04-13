# Changelog

All notable changes to the spvirit workspace are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-04-14

### Added

- **`spvirit-types`**: new `NtPayload::Structure` variant carrying an
  `NtStructure { struct_id, fields: Vec<(String, NtField)> }`. `NtField`
  supports `Scalar`, `ScalarArray`, and recursively nested `Structure`.
  Use this for PVs whose payload is a generic nested structure that does
  not match a canonical Normative Type — primarily QSRV group PVs from
  `epics-bridge-rs`. Unions, struct-arrays, and variant types are
  intentionally not supported (see the `NtStructure` doc comment).
- **`spvirit-codec`**: `nt_structure_desc()` and `encode_nt_structure_full()`
  derive a wire-shape `StructureDesc` and encoded value bytes from any
  `NtStructure`. The dispatch in `nt_payload_desc()` /
  `encode_nt_payload_full()` was extended to route `NtPayload::Structure`
  through these helpers. Endianness is respected at every nesting level.
- **`spvirit-server`**:
  - `RecordData::NtStructure { nt: NtStructure }` variant + matching
    `RecordType::NtStructure` so generic structures are first-class
    records — `SimplePvStore` stores them, monitor notifications fire on
    change, and `PvStore::get_descriptor` returns the correct descriptor
    via the new codec helpers.
  - `PvaServerBuilder::nt_structure(name, NtStructure)` to register a
    structure PV with an initial value.
  - `SimplePvStore` now accepts PUTs whose decoded value is a structure
    and replaces the stored `NtStructure` wholesale (field-level partial
    updates intentionally remain a custom-`PvStore` concern).
  - `examples/generic_structure.rs` demonstrates the end-to-end path.

### Changed (BREAKING)

- **`spvirit-types`**: `NtPayload` is now `#[non_exhaustive]`. Downstream
  `match payload { ... }` expressions must include a wildcard arm. This
  makes the existing breaking change (the new `Structure` variant)
  explicit and lets future variants be added without further breakage.
- All workspace crates (`spvirit-types`, `spvirit-codec`, `spvirit-client`,
  `spvirit-server`, `spvirit-tools`) bumped to `0.2.0` to reflect the
  enum extension. Cargo treats `^0.1.x` and `^0.2.x` as incompatible by
  design; consumers must opt in.

### Migration

If you were on `0.1.6` / `0.1.7` and matched on `NtPayload`, add a
wildcard arm:

```diff
 match payload {
     NtPayload::Scalar(nt)      => ...,
     NtPayload::ScalarArray(nt) => ...,
     NtPayload::Table(nt)       => ...,
     NtPayload::NdArray(nt)     => ...,
+    NtPayload::Structure(nt)   => ...,   // new in 0.2.0
+    _ => ...,                            // required by #[non_exhaustive]
 }
```

If you were *constructing* `NtPayload` you need no changes — the
existing variants are unchanged.

## [0.1.6] and earlier

See `git log` — versions prior to 0.2.0 were patch-level releases without
a maintained CHANGELOG.
