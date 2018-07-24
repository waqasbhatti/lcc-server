# -*- coding: utf-8 -*-

'''setup.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Nov 2016

This sets up the package.

Stolen from http://python-packaging.readthedocs.io/en/latest/everything.html and
modified by me.

'''
__version__ = '0.0.1'

import sys
from setuptools import setup, find_packages

# pytesting stuff and imports copied wholesale from:
# https://docs.pytest.org/en/latest/goodpractices.html#test-discovery
from setuptools.command.test import test as TestCommand
class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def run_tests(self):
        import shlex
        # import here, cause outside the eggs aren't loaded
        import pytest

        if not self.pytest_args:
            targs = []
        else:
            targs = shlex.split(self.pytest_args)

        errno = pytest.main(targs)
        sys.exit(errno)


def readme():
    with open('README.rst') as f:
        return f.read()

INSTALL_REQUIRES = [
    'astrobase>=0.3',
    'astropy',
    'numpy',
    'scipy',
    'tornado',
    'matplotlib',
    'requests',
    'tqdm',
    'psycopg2-binary',
    'markdown',
    'pygments',
    'psutil',
]

EXTRAS_REQUIRE = {
    'postgres':['psycopg2'],
}


###############
## RUN SETUP ##
###############

setup(
    name='lccserver',
    version=__version__,
    description=('A light curve collection server framework.'),
    long_description=readme(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Astronomy",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords='astronomy',
    url='https://github.com/waqasbhatti/lcc-server',
    author='Waqas Bhatti',
    author_email='waqas.afzal.bhatti@gmail.com',
    license='MIT',
    packages=find_packages(),
    install_requires=INSTALL_REQUIRES,
    # extras_require=EXTRAS_REQUIRE,
    tests_require=['pytest',],
    cmdclass={'test':PyTest},
    entry_points={
        'console_scripts':[
            'indexserver=lccserver.frontend.indexserver:main',
            # 'searchserver=lccserver.frontend.searchserver:main',
            # 'lcserver=lccserver.frontend.lcserver:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.4',
)
