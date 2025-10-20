from durus import __version__
import sys, os

from setuptools import setup, Extension

IS_PYPY = hasattr(sys, 'pypy_version_info')

if 'sdist' in sys.argv:
    if sys.platform == 'darwin':
        # Omit extended attributes from tarfile
        os.environ['COPYFILE_DISABLE'] = 'true'

# Only build C extension for CPython
if IS_PYPY:
    ext_modules = []
else:
    persistent = Extension(name="durus._persistent",
                          sources=["durus/_persistent.c"])
    ext_modules = [persistent]

setup(name = "Durus",
      version = __version__,
      description = "A Python Object Database",
      long_description = """
      Serves and manages changes to persistent objects being used in
      multiple client processes.
      """,
      scripts = ["bin/durus"],
      package_dir = {'durus' : 'durus'},
      packages = ["durus"],
      platforms = ['Python >=2.6'],
      author = "CNRI and others",
      author_email = "nas-durus@arctrix.com",
      url = "https://github.com/nascheme/durus",
      ext_modules = ext_modules,
      license = "see LICENSE.txt",
      zip_safe=False,
      )
