#!/usr/bin/env python
import os
import re
from setuptools import setup
from setuptools.command.test import test as TestCommand
import sys

ROOT = os.path.abspath(os.path.dirname(__file__))
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass into py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        try:
            from multiprocessing import cpu_count
            self.pytest_args = ['-n', str(cpu_count()), '--boxed']
        except (ImportError, NotImplementedError):
            self.pytest_args = ['-n', '1', '--boxed']

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


packages = ['s3mon']

requires = [
    'boto3>=1.7',
]

test_requirements = [
    'pytest-cov',
    'pytest>=3'
]

with open('README.md', 'r') as f:
    readme = f.read()

def get_version():
    init = open(os.path.join(ROOT, 's3mon', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)

setup(
    name='s3mon',
    version=get_version(),
    description='New files monitoring of public s3 bucket',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Gustavo Oliveira',
    author_email='cetres@gmail.com',
    url='https://github.com/cetres/s3mon',
    packages=packages,
    package_data={'': ['LICENSE'], 's3mon': ['*.pem']},
    package_dir={'s3mon': 's3mon'},
    scripts=['s3mon.py'],
    include_package_data=True,
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*",
    install_requires=requires,
    license='MIT',
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: System :: Networking :: Monitoring",
    ],
    cmdclass={'test': PyTest},
    tests_require=test_requirements,
    project_urls={
        'Source': 'https://github.com/cetres/s3mon',
    },
)