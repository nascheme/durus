"""
$URL$
$Id$
"""
from durus.__init__ import __version__
from durus.utils import IS_PYPY
import re, sys, os
assert sys.version_info[:2] >= (2, 6)

from setuptools import setup, Extension

from setuptools.command.test import test as TestCommand

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

if 'sdist' in sys.argv:
    if sys.platform == 'darwin':
        # Omit extended attributes from tarfile
        os.environ['COPYFILE_DISABLE'] = 'true'
    # Make sure that version numbers have all been updated.
    PAT = re.compile(r'\b%s\b' % re.escape(__version__))
    assert len(PAT.findall(open("LICENSE.txt").read())) == 14, __version__
    assert PAT.search(open("CHANGES.txt").readline()), __version__
    assert len(PAT.findall(open("INSTALL.txt").read())) == 2, __version__

    # Make sure that copyright statements are current.
    from datetime import datetime
    year = datetime.now().year
    copyright = \
        "Copyright (c) Corporation for National Research Initiatives %s" % year
    assert open("durus/__init__.py").read().count(copyright) == 1
    assert open("README.txt").read().count(copyright) == 1

persistent = Extension(name="durus._persistent", sources=["_persistent.c"])
setup(
    name = "Durus",
    version = __version__,
    description = "A Python Object Database",
    long_description = """
    Serves and manages changes to persistent objects being used in
    multiple client processes.
    """,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Framework :: Durus',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Operating System :: MacOS :: MacOS X',
        ],
    scripts = ["scripts/durus"],
    packages = ["durus"],
    platforms = ['Python >=2.6'],
    author = "CNRI",
    author_email = "webmaster@mems-exchange.org",
    url = "http://www.mems-exchange.org/software/durus/",
    ext_modules = [persistent] if not IS_PYPY else None,
    license = "see LICENSE.txt",
    tests_require=['pytest'],
    cmdclass = {'test': PyTest},
    install_requires=[
        'setuptools',
        ],
      )
