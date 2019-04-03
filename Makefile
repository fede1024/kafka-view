VENV := precommit_venv
HOOKS := .git/hooks/pre-commit

# PRE-COMMIT HOOKS

$(VENV): .requirements-precommit.txt
	virtualenv -p python3 $(VENV)
	$(VENV)/bin/pip install -r .requirements-precommit.txt

.PHONY: env
env: $(VENV)

.PHONY: clean-env
clean-env:
	rm -rf $(VENV)

$(HOOKS): $(VENV) .pre-commit-config.yaml
	$(VENV)/bin/pre-commit install -f --install-hooks
	cargo fmt --help > /dev/null || rustup component add rustfmt
	cargo clippy --help > /dev/null || rustup component add clippy

.PHONY: install-hooks
install-hooks: $(HOOKS)

.PHONY: clean-hooks
clean-hooks:
	rm -rf $(HOOKS)

# LINTING

.PHONY: lint
lint:
	 cargo fmt

.PHONY: clean-lint
clean-lint:
	find . -type f -name *.rs.bk -delete

.PHONY: clippy
clippy:
	cargo clippy --all-features -- -A clippy::match_wild_err_arm
