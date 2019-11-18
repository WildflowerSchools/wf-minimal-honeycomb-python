import os
from setuptools import setup, find_packages

BASEDIR = os.path.dirname(os.path.abspath(__file__))
VERSION = open(os.path.join(BASEDIR, 'VERSION')).read().strip()

BASE_DEPENDENCIES = [
    'wildflower-honeycomb-sdk>=0.7.3',
]

# allow setup.py to be run from any path
os.chdir(os.path.normpath(BASEDIR))

setup(
    name='wf-minimal-honeycomb-python',
    packages=find_packages(),
    version=VERSION,
    include_package_data=True,
    description='An implementation of a minimal Python API for Wildflower\'s Honeycomb database',
    long_description=open('README.md').read(),
    url='https://github.com/WildflowerSchools/wf-minimal-honeycomb-python',
    author='Theodore Quinn',
    author_email='ted.quinn@wildflowerschools.org',
    install_requires=BASE_DEPENDENCIES,
    keywords=['database'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
