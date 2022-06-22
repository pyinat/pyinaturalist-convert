"""Config file for Sphinx documentation"""
from importlib.metadata import version as pkg_version
from pathlib import Path

PACKAGE_DIR = Path(__file__).parent.parent / 'pyinaturalist_convert'
DOCS_DIR = Path(__file__).parent

# General information about the project.
copyright = '2022, Jordan Cook'
exclude_patterns = ['_build']
master_doc = 'index'
project = 'pyinaturalist-convert'
source_suffix = ['.rst', '.md']
version = release = pkg_version('pyinaturalist-convert')

# Sphinx extensions
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinx_copybutton',
    'sphinx_design',
    'myst_parser',
]
myst_enable_extensions = ['colon_fence']

# Enable automatic links to other projects' Sphinx docs
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'requests': ('https://requests.readthedocs.io/en/stable/', None),
    'pyinaturalist': ('https://pyinaturalist.readthedocs.io/en/stable/', None),
    'tablib': ('https://tablib.readthedocs.io/en/stable/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
}

# napoleon settings
napoleon_google_docstring = True
napoleon_include_init_with_doc = True
numpydoc_show_class_members = False

# copybutton settings: Strip prompt text when copying code blocks
copybutton_prompt_text = r'>>> |\.\.\. |\$ '
copybutton_prompt_is_regexp = True

# Disable autodoc's built-in type hints, and use sphinx_autodoc_typehints extension instead
autodoc_typehints = 'none'

# HTML general settings
# html_static_path = ['_static']
html_show_sphinx = False
pygments_style = 'friendly'
pygments_dark_style = 'material'

# HTML theme settings
html_theme = 'furo'
# html_logo = '_static/logo.png'
# html_theme_options = {'sidebar_hide_name': True}
