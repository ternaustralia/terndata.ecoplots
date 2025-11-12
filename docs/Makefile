# Minimal Sphinx Makefile (docs/Makefile)
#
# Quick Start:
#   make html      - Build HTML documentation
#   make serve     - Serve built docs on http://localhost:9080/index.html
#   make live      - Live preview with auto-rebuild (opens browser)
#
# Note: If you see a blank page, manually navigate to:
#   http://localhost:9080/index.html

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
	@echo "Sphinx documentation build targets:"
	@echo ""
	@echo "  make html              - Build HTML documentation"
	@echo "  make serve             - Serve pre-built HTML on http://localhost:9080"
	@echo "  make live              - Live preview with auto-reload (requires sphinx-autobuild)"
	@echo "  make clean             - Remove build artifacts (preserves api/ docs)"
	@echo "  make apidoc            - Verify API documentation files are present"
	@echo "  make init-api          - Initialize/recreate API documentation structure"
	@echo "  make linkcheck         - Check external links"
	@echo "  make doctest           - Run doctests in documentation"
	@echo ""
	@echo "API Documentation:"
	@echo "  We maintain 3 manually-crafted API docs in api/ directory:"
	@echo "    - overview.rst           (package overview from __init__.py)"
	@echo "    - ecoplots_class.rst     (EcoPlots synchronous class)"
	@echo "    - async_ecoplots_class.rst (AsyncEcoPlots asynchronous class)"
	@echo ""
	@echo "Other Sphinx modes (advanced):"
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(O)

# Clean build + generated stubs (but preserve manually-crafted api/ docs)
.PHONY: clean
clean:
	@rm -rf "$(BUILDDIR)" "_autosummary"
	@echo "Cleaned build artifacts (preserved api/ directory with manual API docs)"

# Build HTML (does not require apidoc if you use autosummary in api.rst)
.PHONY: html
html:
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(O)
	@echo ""
	@echo "✓ HTML documentation built successfully!"
	@echo "  To view locally: open $(BUILDDIR)/html/html/index.html"
	@echo "  Or run: make serve"

# Serve pre-built HTML on a simple HTTP server (requires docs to be built first)
.PHONY: serve
serve:
	@if [ ! -d "$(BUILDDIR)/html/html" ]; then \
		echo "ERROR: No built documentation found. Run 'make html' first."; \
		exit 1; \
	fi
	@echo "Serving documentation on http://localhost:9999/index.html"
	@echo "Press Ctrl+C to stop the server"
	@echo "Opening browser..."
	@open http://localhost:9999/index.html 2>/dev/null || xdg-open http://localhost:9999/index.html 2>/dev/null || echo "Please open http://localhost:9999/index.html in your browser"
	@cd $(BUILDDIR)/html/html && python3 -m http.server 9999

# Live preview (auto-reload). Requires sphinx-autobuild.
.PHONY: live
live:
	@echo "Starting live preview server on http://localhost:9999/index.html"
	@echo "The documentation will auto-rebuild on file changes..."
	@$(SPHINXAUTOBUILD) "$(SOURCEDIR)" "$(BUILDDIR)/html" \
		--host 0.0.0.0 \
		--port 9999 \
		--open-browser \
		--ignore "*.swp" \
		--ignore "*.swx" \
		--ignore "*~" \
		--watch ../src/terndata/ecoplots \
		--re-ignore "_build" \
		--delay 1

# Verify that manually-crafted API docs are in place
# These files document: overview (__init__.py), EcoPlots class, AsyncEcoPlots class
.PHONY: apidoc
apidoc:
	@echo "Checking manually-crafted API documentation files..."
	@if [ ! -f "$(API_OUT)/overview.rst" ]; then \
		echo "ERROR: $(API_OUT)/overview.rst is missing!"; \
		echo "This file should document the package overview from __init__.py"; \
		exit 1; \
	fi
	@if [ ! -f "$(API_OUT)/ecoplots_class.rst" ]; then \
		echo "ERROR: $(API_OUT)/ecoplots_class.rst is missing!"; \
		echo "This file should document the EcoPlots synchronous class"; \
		exit 1; \
	fi
	@if [ ! -f "$(API_OUT)/async_ecoplots_class.rst" ]; then \
		echo "ERROR: $(API_OUT)/async_ecoplots_class.rst is missing!"; \
		echo "This file should document the AsyncEcoPlots asynchronous class"; \
		exit 1; \
	fi
	@echo "✓ All required API documentation files are present:"
	@echo "  - $(API_OUT)/overview.rst (package overview)"
	@echo "  - $(API_OUT)/ecoplots_class.rst (EcoPlots class)"
	@echo "  - $(API_OUT)/async_ecoplots_class.rst (AsyncEcoPlots class)"

# Auto-generate API stubs using sphinx-apidoc (DEPRECATED - we use manual docs)
# This target is kept for reference but should not be used normally
.PHONY: apidoc-auto
apidoc-auto:
	@echo "WARNING: This target auto-generates API docs and may overwrite manual files!"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	@mkdir -p "$(API_OUT)"
	@$(SPHINXAPIDOC) -f -o "$(API_OUT)" \
		"$(PKGDIR)" \
		"$(PKGDIR)/ecoplots/_*" \
		"$(PKGDIR)/ecoplots/assets*"

# Initialize API documentation structure (creates the 3 required files)
# Use this target to set up API docs from scratch
.PHONY: init-api
init-api:
	@echo "Creating API documentation structure..."
	@mkdir -p "$(API_OUT)"
	@echo "Package Overview" > "$(API_OUT)/overview.rst"
	@echo "================" >> "$(API_OUT)/overview.rst"
	@echo "" >> "$(API_OUT)/overview.rst"
	@echo ".. automodule:: terndata.ecoplots" >> "$(API_OUT)/overview.rst"
	@echo "   :members:" >> "$(API_OUT)/overview.rst"
	@echo "   :undoc-members:" >> "$(API_OUT)/overview.rst"
	@echo "   :show-inheritance:" >> "$(API_OUT)/overview.rst"
	@echo "" >> "$(API_OUT)/overview.rst"
	@echo "EcoPlots" > "$(API_OUT)/ecoplots_class.rst"
	@echo "======================" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "" >> "$(API_OUT)/ecoplots_class.rst"
	@echo ".. autoclass:: terndata.ecoplots.ecoplots.EcoPlots" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "   :members:" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "   :undoc-members:" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "   :inherited-members: terndata.ecoplots._base.EcoPlotsBase" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "   :show-inheritance:" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "   :member-order: bysource" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "" >> "$(API_OUT)/ecoplots_class.rst"
	@echo "AsyncEcoPlots" > "$(API_OUT)/async_ecoplots_class.rst"
	@echo "============================" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo ".. autoclass:: terndata.ecoplots.ecoplots.AsyncEcoPlots" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "   :members:" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "   :undoc-members:" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "   :inherited-members: terndata.ecoplots._base.EcoPlotsBase" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "   :show-inheritance:" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "   :member-order: bysource" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "" >> "$(API_OUT)/async_ecoplots_class.rst"
	@echo "✓ Created API documentation structure in $(API_OUT)/"
	@echo "  - overview.rst (package overview from __init__.py)"
	@echo "  - ecoplots_class.rst (EcoPlots synchronous class)"
	@echo "  - async_ecoplots_class.rst (AsyncEcoPlots asynchronous class)"

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
%:
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(O)
