import sys
from datetime import datetime, timezone
from pathlib import Path

# Make src/ importable on RTD/local
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Debug: Print Python path for troubleshooting
print(f"[DEBUG] Python path: {sys.path[:3]}")
print(f"[DEBUG] ROOT: {ROOT}")
print(f"[DEBUG] SRC: {SRC}")

# Test import to catch errors early
try:
    import terndata.ecoplots
    print(f"[DEBUG] Successfully imported terndata.ecoplots from {terndata.ecoplots.__file__}")
except ImportError as e:
    print(f"[ERROR] Failed to import terndata.ecoplots: {e}")

project = "terndata.ecoplots"
author = "Avinash Chandra"

START_YEAR = 2025
YEAR = datetime.now(timezone.utc).year
years = f"{START_YEAR}-{YEAR}" if YEAR > START_YEAR else f"{YEAR}"
copyright = f"{years}, TDSA (TERN Data Services and Analytics)"  # noqa: A001
release = "0.0.3-beta2"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx.ext.githubpages",  # needed for GitHub Pages
]

html_theme = "furo"

# HTML / assets
html_static_path = ["_static"]
html_css_files = ["brand.css"]
html_favicon = "_static/img/ecoplots-logo-dark.svg"

# Sidebar branding
html_theme_options = {
    # Sidebar logo with light/dark variants
    "light_logo": "img/TERN-logo-primary.png",
    "dark_logo": "img/TERN-logo-reversed.png",
    "sidebar_hide_name": True,
    "light_css_variables": {
        "color-brand-primary": "#00565D",
        "color-brand-content": "#00565D",
    },
    "dark_css_variables": {
        "color-brand-primary": "#6EB3A6",
        "color-brand-content": "#6EB3A6",
    },
}


# Autodoc/Autosummary
autosummary_generate = True
autodoc_typehints = "description"
autodoc_default_options = {"members": True, "undoc-members": False}
# Google style
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_param = True
napoleon_use_rtype = False

napoleon_custom_sections = [
    "Public API",
    "Quick Start",
    "Side Effects",
]

autodoc_default_options = {
    "undoc-members": False,
    "private-members": False,  # don't show _private names
    "imported-members": False,  # don't show imported members
    "member-order": "bysource",
}

# Make __all__ control what is considered public in modules
autosummary_ignore_module_all = False

autosummary_generate = True
autodoc_typehints = "description"


# Mock heavy/GUI deps so RTD can import modules
autodoc_mock_imports = [
    "ipyleaflet",
    "ipywidgets",
    "shapely",
    "geopandas",
    "fiona",
    "pyproj",
    "rtree",
]

exclude_patterns = ["api/modules.rst"]
