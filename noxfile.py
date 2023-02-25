
import nox

nox.options.sessions = [
    "build-docs",
    "unit-tests"
]

@nox.session(name="build-docs")
def build_docs(session: nox.Session):
    """Build the documentation.
    """
    dev_venv_setup(session=session)
    session.run("rm", "-rf", "./docs/_build/", 
        external=True
    )
    session.run("sphinx-build", "-b", "html", "./docs", "./docs/_build")


@nox.session(name="docs-server")
def docs_server(session: nox.Session):
    """Run a local server for the docs at http://localhost:7999/index.html
    """
    session.run("python", "-m", "http.server", "-d", "docs/_build/html/", "7999")


@nox.session(name="unit-tests-curr-python")
def unit_tests_current_python(session: nox.Session):
    """Run tests with current python version and generate html coverage report.
    """
    dev_venv_setup(session=session)
    session.run("coverage", "erase")
    session.run("pytest", "-vvv", "--cov=src/beatdrop", "--cov-report", "html", "tests/unit")


@nox.session(
    name="unit-tests",
    python=[
        "3.7",
        "3.8",
        "3.9",
        "3.10",
        "3.11"
    ]
)
def unit_tests(session: nox.Session):
    """Run tests with all supported python version and generate missing coverage report in terminal.
    """
    dev_venv_setup(session=session)
    session.run("coverage", "erase")
    session.run("pytest", "-vvv", "--cov=src/beatdrop", "--cov-report", "term-missing", "tests/unit")


def dev_venv_setup(session: nox.Session):
    session.install("-U", "pip")
    session.install("-e", ".[dev,all]")

