"""Config file for Sphinx documentation"""
from importlib.metadata import version as pkg_version
from pathlib import Path

PACKAGE_DIR = Path(__file__).parent.parent / 'pyinaturalist_convert'
DOCS_DIR = Path(__file__).parent
TEMPLATE_DIR = DOCS_DIR / '_templates'

# General information about the project.
copyright = '2022, Jordan Cook'
exclude_patterns = ['_build']
master_doc = 'index'
needs_sphinx = '4.0'
project = 'pyinaturalist-convert'
source_suffix = ['.rst', '.md']
templates_path = ['_templates']
version = release = pkg_version('pyinaturalist-convert')

# Sphinx extensions
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinx_automodapi.automodapi',
    'sphinx_automodapi.smart_resolver',
    'sphinx_copybutton',
    'sphinx_inline_tabs',
    'sphinx_panels',
    'sphinxcontrib.apidoc',
    'myst_parser',
]
myst_enable_extensions = ['colon_fence']

# Enable automatic links to other projects' Sphinx docs
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'requests': ('https://docs.python-requests.org/en/stable/', None),
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

# apidoc settings
apidoc_module_dir = str(PACKAGE_DIR)
apidoc_output_dir = 'modules'
apidoc_extra_args = [f'--templatedir={TEMPLATE_DIR}']  # Note: Must be an absolute path
apidoc_module_first = True
apidoc_separate_modules = True
apidoc_toc_file = False
exclude_patterns = [
    'modules/pyinaturalist_convert.rst',
    'modules/pyinaturalist_convert.constants.rst',
]


# HTML general settings
# html_static_path = ['_static']
html_show_sphinx = False
pygments_style = 'friendly'
pygments_dark_style = 'material'

# HTML theme settings
html_theme = 'furo'
# html_logo = '_static/logo.png'
# html_theme_options = {'sidebar_hide_name': True}


def setup(app):
    """Run some additional steps after the Sphinx builder is initialized"""
    app.connect('builder-inited', patch_automodapi)


def patch_automodapi(app):
    """Monkey-patch the automodapi extension to exclude imported members.

    https://github.com/astropy/sphinx-automodapi/blob/master/sphinx_automodapi/automodsumm.py#L135
    """
    from sphinx_automodapi import automodsumm
    from sphinx_automodapi.utils import find_mod_objs

    automodsumm.find_mod_objs = lambda *args: find_mod_objs(args[0], onlylocals=True)
