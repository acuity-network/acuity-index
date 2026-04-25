# Overview

`acuity-index` is a configurable event indexer for Substrate-based blockchains.
It connects to a node over WebSocket RPC, decodes runtime events, stores
queryable index entries in `sled`, and exposes the indexed data through its own
WebSocket API.

The project is intentionally config-driven:

- chain-specific indexing rules live in TOML rather than generated Rust types
- event payloads are decoded generically
- the on-disk index is built around explicit query keys

## Who This Book Is For

This book is split across three overlapping audiences:

- operators running `acuity-index` against a live chain
- contributors changing the codebase or test harness
- readers who want to understand the internal architecture

## What To Read First

- If you want to run the indexer, start with [Installation](./installation.md),
  [Quickstart](./quickstart.md), and [Operations](./operations.md).
- If you want to define a chain spec, go to [Configuration](./configuration.md).
- If you want to integrate with the service, read [WebSocket API](./api.md).
- If you want to work on the codebase, read [Contributing](./contributing.md)
  and [Synthetic Devnet](./synthetic-devnet.md).
- If you want the implementation map, read [Architecture](./architecture.md).

## Core Properties

- Config-driven indexing with explicit pallet and event mappings
- Resume-safe span tracking across restarts
- Hot reload for accepted index-spec changes
- Concurrent backfill and head catch-up via configurable queue depth
- Public JSON-over-WebSocket query interface
- Optional finalized proof responses for `GetEvents`
- In-repo synthetic runtime for integration testing and benchmarking
