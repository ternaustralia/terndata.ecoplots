# Minimal Sphinx Makefile (docs/Makefile)

# Tools (override via env if needed)
SPHINXBUILD    ?= sphinx-build
SPHINXAUTOBUILD?= sphinx-autobuild
SPHINXAPIDOC   ?= sphinx-apidoc

# Layout
SOURCEDIR      = .
BUILDDIR       = _build

# Your package (src layout)
PKGDIR         = ../src/terndata
# Where to write generated .rst files
API_OUT        = api
API_INT_OUT    = api_internal

# Default target
.PHONY: help
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(O)

# Clean build + generated stubs
.PHONY: clean
clean:
	@rm -rf "$(BUILDDIR)" "$(API_OUT)" "$(API_INT_OUT)" "_autosummary"

# Build HTML (does not require apidoc if you use autosummary in api.rst)
.PHONY: html
html:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(O)

# Live preview (auto-reload). Requires sphinx-autobuild.
.PHONY: live
live:
	@$(SPHINXAUTOBUILD) "$(SOURCEDIR)" "$(BUILDDIR)/html" --host 0.0.0.0 --open-browser

# Generate API stubs for PUBLIC modules (skips _private and assets)
# Adjust excludes as needed.
.PHONY: apidoc
apidoc:
	@mkdir -p "$(API_OUT)"
	@$(SPHINXAPIDOC) -f -o "$(API_OUT)" \
		"$(PKGDIR)" \
		"$(PKGDIR)/ecoplots/_*" \
		"$(PKGDIR)/ecoplots/assets*"

# Generate API stubs INCLUDING private modules (for maintainers)
.PHONY: apidoc-internal
apidoc-internal:
	@mkdir -p "$(API_INT_OUT)"
	@$(SPHINXAPIDOC) -f -P -o "$(API_INT_OUT)" "$(PKGDIR)"

# Link checker & doctest (nice-to-have)
.PHONY: linkcheck doctest
linkcheck:
	@$(SPHINXBUILD) -M linkcheck "$(SOURCEDIR)" "$(BUILDDIR)" $(O)

doctest:
	@$(SPHINXBUILD) -M doctest "$(SOURCEDIR)" "$(BUILDDIR)" $(O)

# Pass-through catch-all to Sphinx make mode (e.g., "make dirhtml", "make latexpdf")
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(O)
