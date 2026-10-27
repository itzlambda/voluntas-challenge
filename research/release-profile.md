# Collector release profile

The root [Cargo.toml](../Cargo.toml) configures the portable release build:

```toml
[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
```

Build with `cargo build --release`. Other release settings retain Cargo defaults.

ThinLTO enables optimization across crate boundaries. One codegen unit gives
LLVM visibility across each crate, at the cost of less parallel compilation.
Optimization level 3 is Cargo's normal release default. These settings were
adopted without a controlled comparison; no throughput or latency gain is claimed.

The collector includes JSON decoding, book maintenance, channels, and generated
SBE encoding, so a parser-only benchmark would not validate the whole pipeline.
The [writer findings](experiments/m5-writer-profile-findings.md) also show that
compiler settings cannot remove synchronous output stalls. Historical live runs
have different traffic and failure conditions and do not isolate compiler effects.

See [Cargo profiles](https://doc.rust-lang.org/cargo/reference/profiles.html)
for setting semantics and [MEASUREMENTS.md](../MEASUREMENTS.md) for measured
results and their limits.
