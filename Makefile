.DEFAULT_GOAL := all
pysources = mt_asyncio tests

.PHONY: build-dev
build-dev:
	@rm -f mt_asyncio/*.so
	uv sync --group all
	maturin develop --uv

# benchmarks MUST run against this, not build-dev: a debug build is several
# times slower and the bench scripts refuse to run on one
.PHONY: build-release
build-release:
	@rm -f mt_asyncio/*.so
	maturin develop --uv --release

.PHONY: format
format:
	ruff check --fix $(pysources)
	ruff format $(pysources)
	cargo fmt

.PHONY: lint-python
lint-python:
	ruff check $(pysources)
	ruff format --check $(pysources)

.PHONY: lint-rust
lint-rust:
	cargo fmt --version
	cargo fmt --all -- --check
	cargo clippy --version
	cargo clippy --tests -- \
		-D warnings \
		-W clippy::pedantic \
		-W clippy::dbg_macro \
		-A clippy::blocks_in_conditions \
		-A clippy::cast-possible-truncation \
		-A clippy::cast-sign-loss \
		-A clippy::declare-interior-mutable-const \
		-A clippy::inline-always \
		-A clippy::match-bool \
		-A clippy::match-same-arms \
		-A clippy::module-name-repetitions \
		-A clippy::needless-pass-by-value \
		-A clippy::no-effect-underscore-binding \
		-A clippy::similar-names \
		-A clippy::single-match-else \
		-A clippy::too-many-arguments \
		-A clippy::too-many-lines \
		-A clippy::type-complexity \
		-A clippy::unused-self \
		-A clippy::upper-case-acronyms \
		-A clippy::used-underscore-binding \
		-A clippy::used-underscore-items \
		-A clippy::wrong-self-convention

.PHONY: lint
lint: lint-python lint-rust

.PHONY: test
test:
	pytest -v tests

# stdlib asyncio vs mt_asyncio.asyncio, same coroutines under both loops.
# BENCH_ARGS passes through, e.g. `make bench BENCH_ARGS="-w mixed --threads 1 8"`
.PHONY: bench
bench:
	python bench/asyncio_bench.py --json bench/results/asyncio.json $(BENCH_ARGS)

.PHONY: bench-quick
bench-quick:
	python bench/asyncio_bench.py --scale 0.1 --repeat 1 --warmup 0 --threads 1 4 $(BENCH_ARGS)

# mt_asyncio vs upstream TonIO from PyPI: has the shared Rust core regressed?
.PHONY: bench-regression
bench-regression:
	python bench/tonio_regression.py --json bench/results/regression.json $(BENCH_ARGS)

.PHONY: all
all: format build-dev lint test
