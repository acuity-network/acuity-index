generate-chain-configs:
	cargo build --release
	./target/release/acuity-index generate-chain-config --url wss://rpc.polkadot.io:443 chains/polkadot.toml
	./target/release/acuity-index generate-chain-config --url wss://kusama-rpc.polkadot.io:443 chains/kusama.toml
	./target/release/acuity-index generate-chain-config --url wss://westend-rpc.polkadot.io:443 chains/westend.toml
	./target/release/acuity-index generate-chain-config --url wss://paseo.ibp.network:443 chains/paseo.toml
