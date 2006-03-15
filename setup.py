"""
$URL$
$Id$
"""
try:
    import setuptools
    used = setuptools # to quiet import checker.
except ImportError:
    pass

import os
from distutils.core import setup
from distutils.extension import Extension

persistent = Extension(name="durus._persistent", sources=["_persistent.c"])

setup(name = "Durus",
      version = "3.3",
      description = "A Python Object Database",
      long_description = """
      Serves and manages changes to persistent objects being used in
      multiple client processes. 
      """,
      scripts = ["durus"],
      package_dir = {'durus' : os.curdir},
      packages = ["durus"],
      platforms = ['Python >2.3'],
      author = "CNRI",
      author_email = "webmaster@mems-exchange.org",
      url = "http://www.mems-exchange.org/software/durus/",
      ext_modules = [persistent],
      license = "see LICENSE.txt",
      )
