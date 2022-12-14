.PHONY: fmt
fmt:
	find -type f -name "*.rs" -not -path "*target*" -exec rustfmt +nightly --edition 2021 {} \;

.PHONY: lint
lint:
	cargo +nightly clippy --fix -Z unstable-options --release --all