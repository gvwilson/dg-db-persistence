skip_psql_tests = True

def pytest_addoption(parser):
    parser.addoption("--psql", action="store_true", help="Run PostgreSQL-specific tests")

def pytest_configure(config):
    global skip_psql_tests
    skip_psql_tests = not config.getoption("--psql")
