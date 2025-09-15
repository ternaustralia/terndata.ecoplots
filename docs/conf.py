import sys
from datetime import datetime
from pathlib import Path

# Make src/ importable on RTD/local
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

project = "terndata.ecoplots"
author = "Avinash Chandra"
copyright = f"{datetime.now():%Y}, {author}"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx.ext.githubpages", # needed for GitHub Pages
]

html_theme = "furo"

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
    "private-members": False,     # don't show _private names
    "imported-members": False,    # don't show imported members
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
    "numpy",
    "pandas",
]

exclude_patterns = ["api/modules.rst"]
