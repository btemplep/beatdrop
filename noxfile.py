
import nox


@nox.session(name="unit-tests-curr-python")
def unit_tests_curr_python(session: nox.Session):
    """Run tests with current python version and generate html coverage report.
    """
    session.install("-U", "pip")
    session.install("-e", ".[dev,all]")
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
    session.install("-U", "pip")
    session.install("-e", ".[dev,all]")
    session.run("coverage", "erase")
    session.run("pytest", "-vvv", "--cov=src/beatdrop", "--cov-report", "term-missing", "tests/unit")

