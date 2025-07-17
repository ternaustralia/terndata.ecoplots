IMAGE=python:latest

.PHONY: build check-types check-lint check doc

build:
	rm -fr $(CURDIR)/dist/*
	python3 -m build

check-types:
	tox -e type

check-lint:
	tox -e lint

check: check-lint check-types

doc:
	docker run --rm -it --env-file envvars \
	  -v $(CURDIR):/workspace \
	  -w /workspace \
	  -u root \
	  --entrypoint ./docs/run-docs.sh \
	  $(IMAGE) \
	  html

test:
	docker run --rm -it \
	  -v $(CURDIR):/workspace \
	  -w /workspace \
	  -u root \
	  --entrypoint ./ci-scripts/run-tests.sh \
	  $(IMAGE)
