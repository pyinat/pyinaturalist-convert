"""Notes:
* 'test' command: nox will use poetry.lock to determine dependency versions
* 'lint' command: tools and environments are managed by pre-commit
* All other commands: the current environment will be used instead of creating new ones
"""

from os.path import join
from shutil import rmtree

import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ['lint', 'cov']

CLEAN_DIRS = ['dist', 'build', join('docs', '_build')]
LIVE_DOCS_PORT = 8181
LIVE_DOCS_IGNORE = ['*.csv', '*.ipynb', '*.pyc', '*.tmp', '**/modules/*']
DEFAULT_COVERAGE_FORMATS = ['html', 'term']
DOC_BUILD_DIR = join('docs', '_build', 'html')


def install_deps(session):
    """Install project and test dependencies into a nox session using uv"""
    session.env['UV_PROJECT_ENVIRONMENT'] = session.virtualenv.location
    session.run_install(
        'uv',
        'sync',
        '--frozen',
        '--all-extras',
    )


@nox.session(python=['3.10', '3.11', '3.12', '3.13'], venv_backend='uv')
def test(session):
    """Run tests for a specific python version"""
    test_paths = session.posargs or ['test']
    install_deps(session)
    session.run('pytest', '-n', 'auto', *test_paths)


@nox.session(python=False)
def clean(session):
    """Clean up temporary build + documentation files"""
    for dir in CLEAN_DIRS:
        print(f'Removing {dir}')
        rmtree(dir, ignore_errors=True)


@nox.session(python=False, name='cov')
def coverage(session):
    """Run tests and generate coverage report"""
    cmd = ['pytest', '-n', 'auto', '--cov']

    # Add coverage formats
    cov_formats = session.posargs or DEFAULT_COVERAGE_FORMATS
    cmd += [f'--cov-report={f}' for f in cov_formats]
    session.run(*cmd)


@nox.session(python=False)
def docs(session):
    """Build Sphinx documentation"""
    cmd = 'sphinx-build docs docs/_build/html -j auto'
    session.run(*cmd.split(' '))


@nox.session(python=False)
def livedocs(session):
    """Auto-build docs with live reload in browser.
    Add `-- open` to also open the browser after starting.
    """
    cmd = ['sphinx-autobuild', 'docs', DOC_BUILD_DIR]
    cmd += ['-a']
    cmd += ['--host', '0.0.0.0']
    cmd += ['--port', str(LIVE_DOCS_PORT), '-j', 'auto']
    cmd += ['--watch', 'pyinaturalist_convert']
    for pattern in LIVE_DOCS_IGNORE:
        cmd += ['--ignore', pattern]
    if session.posargs == ['open']:
        cmd.append('--open-browser')

    clean(session)
    session.run(*cmd)


@nox.session(python=False)
def lint(session):
    """Run linters and code formatters via pre-commit"""
    cmd = 'pre-commit run --all-files'
    session.run(*cmd.split(' '))
