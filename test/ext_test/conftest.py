import pytest
import os 

def pytest_addoption(parser):
    parser.addoption("--archive_backend", type=str, default=None,                      
                     help="URL of the backend rest API")

#def pytest_generate_tests(metafunc):
#    if "archive_backend" in metafunc.fixturenames:
#        metafunc.parametrize("archive_backend", pytest.param(metafunc.config.getoption("archive_backend"),marks=pytest.mark.skipif('metafunc.config.getoption("archive_backend") is None',reason="No backend specified on command line.")))

@pytest.fixture
def archive_backend(request):
    backend = request.config.getoption("--archive_backend")
    if backend is None:
        pytest.skip()
    return backend

