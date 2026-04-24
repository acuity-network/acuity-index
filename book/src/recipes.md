# Recipe Reference

## Run Against A Live Node

```bash
acuity-index run ./mychain.toml --url wss://mynode:443
```

## Generate A Starter Spec

```bash
acuity-index generate-index-spec ./mychain.toml --url wss://mynode:443
```

## Purge An Existing Index

```bash
acuity-index purge-index ./mychain.toml
```

## Start The Synthetic Node

```bash
just synthetic-node
```

## Seed Synthetic Smoke Data

```bash
just seed-smoke
```

## Run Ignored Integration Tests

```bash
just test-integration
```

## Build The Book

```bash
just book-build
```

## Serve The Book Locally

```bash
just book-serve
```
