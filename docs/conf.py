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

templates_path = ["_templates"]

html_theme = "furo"

html_sidebars = {
    "**": [
        "sidebar/brand.html",
        "sidebar/search.html",
        "sidebar/navigation.html",
        "sidebar/github-link.html",
        "sidebar/ethical-ads.html",
    ]
}

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
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/ebonnal/streamable",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path>
                </svg>
            """,
            "class": "",
        },
    ],
}

# Better links to headings when using MyST Markdown
myst_heading_anchors = 3

todo_include_todos = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Type hints in the signature and/or in the description:
typehints_fully_qualified = False
typehints_use_signature = True

python_maximum_signature_line_length = 1
