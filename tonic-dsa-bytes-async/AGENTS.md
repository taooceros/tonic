# tonic-dsa-bytes-async AGENTS

Inherits `../AGENTS.md`.

## CRATE JOB
- Provide opt-in async Intel DSA byte-copy support for tonic encode paths.
- Keep the raw `Bytes` codec focused on copying payload bytes through DSA without protobuf-specific behavior.
- Keep DSA queue setup, descriptor submission, completion polling, and page-fault retry handling behind this crate boundary.

## TEST OWNERSHIP
- Raw bytes codec hardware smoke tests belong in this crate.
- Generated protobuf field-shape/e2e tests belong in `../prost/tests` and should exercise Prost's async payload-copy hook.
- Hardware smoke tests use `TONIC_DSA_WQ`; run them through `dsa_launcher` when direct access lacks capability.
