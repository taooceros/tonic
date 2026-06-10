# Agent Notes

## Primary direction: DSA integration toward tonic

This workspace is for moving Intel DSA codec work toward tonic without turning
tonic into a hardware-specific fork.

- Keep `tonic` and `tonic-prost` upstream-shaped: generic codec APIs, stable
  call sites, and CPU behavior as the default path.
- Use `tonic-dsa-bytes-sync` as the current experimental DSA/bytes crate.
  Keep prost-specific DSA work aside until the raw byte/`Buf` codec boundary is
  proven useful.
- DSA paths must stay opt-in. Device paths, shared work queues, descriptor
  polling, and other `idxd` details belong behind the DSA crate boundary until a
  tonic-level abstraction is justified.
- Do not run hardware-facing binaries directly when the documented flow requires
  `launch` or `dsa_launcher`.
- Preserve the ordinary `tonic-prost` codec as the baseline. When changing codec
  hot paths, compare against the recorded benchmark baseline before claiming a
  performance win or no regression.
- Prefer clean cutovers once an abstraction is proven. Remove temporary shims,
  duplicated codec paths, and compatibility wrappers that no longer carry their
  weight.

## Codec performance checks

For async codec performance checks, use the recorded baseline file instead of
rerunning `origin/master` every time:

- Baseline file: `tonic/benches/decode-origin-master-4687108.bench`
- Benchmark command for the current branch: `cargo bench -p tonic --bench decode`

Workflow:
1. Run the benchmark on the current branch.
2. Compare the output to the recorded baseline file.
3. Rerun `origin/master` only when the benchmark file is stale, the benchmark
   changed, dependency resolution changed, or the run is on meaningfully
   different hardware/toolchain.
