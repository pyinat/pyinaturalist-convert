"""Notes:
* 'test' command: nox will use poetry.lock to determine dependency versions
* 'lint' command: tools and environments are managed by pre-commit
* All other commands: the current environment will be used instead of creating new ones
"""
import nox
from nox_poetry import session

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ['lint', 'cov']

DEFAULT_COVERAGE_FORMATS = ['html', 'term']


@session(python=['3.8', '3.9', '3.10'])
def test(session):
    """Run tests for a specific python version"""
    test_paths = session.posargs or ['test']
    session.install('.', 'pytest', 'pytest-xdist')
    session.run('pytest', '-n', 'auto', *test_paths)


@session(python=False, name='cov')
def coverage(session):
    """Run tests and generate coverage report"""
    cmd = ['pytest', '-n', 'auto', '--cov']

    # Add coverage formats
    cov_formats = session.posargs or DEFAULT_COVERAGE_FORMATS
    cmd += [f'--cov-report={f}' for f in cov_formats]
    session.run(*cmd)


@session(python=False)
def lint(session):
    """Run linters and code formatters via pre-commit"""
    cmd = 'pre-commit run --all-files'
    session.run(*cmd.split(' '))
