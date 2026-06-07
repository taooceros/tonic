# Agent Notes

## Decode benchmark baseline

For async codec performance checks, use the recorded baseline file instead of rerunning `origin/master` every time:

- Baseline file: `tonic/benches/decode-origin-master-4687108.bench`
- Benchmark command for the current branch: `cargo bench -p tonic --bench decode`

Workflow:
1. Run the benchmark on the current branch.
2. Compare the output to the recorded baseline file.
3. Rerun `origin/master` only when the benchmark file is stale, the benchmark changed, dependency resolution changed, or the run is on meaningfully different hardware/toolchain.
