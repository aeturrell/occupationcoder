#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['pandas',
                'scikit-learn',
                'nltk',
                'dask',
                'beautifulsoup4']

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Arthur Turrell and Jyldyz Djumalieva",
    author_email='',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="A tool to assign standard occupational classification codes to job vacancy descriptions",
    install_requires=requirements,
    license="Custom",
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/x-rst',
    include_package_data=True,
    keywords='occupationcoder',
    name='occupationcoder',
    packages=find_packages(include=['occupationcoder', 'occupationcoder.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/aeturrell/occupationcoder',
    version='0.2.0',
    zip_safe=False,
)
