# Sphinx configuration for streamable
import importlib.metadata
from datetime import datetime

project = "streamable"
author = "ebonnal"
copyright = f"{datetime.now():%Y}, {author}"

try:
    release = version = importlib.metadata.version("streamable")
except Exception:
    release = version = "0.0.0"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

autosummary_generate = True

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "inherited-members": True,
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_param = True

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

html_theme = "furo"

# Better links to headings when using MyST Markdown
myst_heading_anchors = 3
