import sys

def pytest_addoption(parser):
	if '--slow' not in sys.argv:
		print('--- to run the slow tests, add "--slow" option')
	else:
		print('slow tests are enabled')
	parser.addoption('--slow', action="store_true", default=False)