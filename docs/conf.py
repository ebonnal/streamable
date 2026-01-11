import importlib.metadata
from datetime import datetime

project = "༄ <code>streamable</code>"
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
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "sphinx.ext.githubpages",
    "sphinx_copybutton",
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

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#4f46e5",
        "color-brand-content": "#1f2933",
        "color-background-primary": "#ffffff",
    },
    "dark_css_variables": {
        "color-brand-primary": "#a5b4fc",
        "color-brand-content": "#e5e7eb",
        "color-background-primary": "#020617",
    },
}

# Better links to headings when using MyST Markdown
myst_heading_anchors = 3

todo_include_todos = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", {}),
}

# Type hints in the signature and/or in the description:
typehints_fully_qualified = False
typehints_use_signature = True

python_maximum_signature_line_length = 1
