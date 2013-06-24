"""
$URL$
$Id$
"""
from durus.__init__ import __version__
import re, sys, os
assert sys.version >= "2.4"

try:
    assert 'USE_DISTUTILS' not in os.environ
    from setuptools import setup, Extension
except (ImportError, AssertionError):
    from distutils.core import setup
    from distutils.extension import Extension

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
setup(name = "Durus",
      version = __version__,
      description = "A Python Object Database",
      long_description = """
      Serves and manages changes to persistent objects being used in
      multiple client processes. 
      """,
      scripts = ["scripts/durus"],
      packages = ["durus"],
      platforms = ['Python >=2.4'],
      author = "CNRI",
      author_email = "webmaster@mems-exchange.org",
      url = "http://www.mems-exchange.org/software/durus/",
      ext_modules = [persistent],
      license = "see LICENSE.txt",
      )
