# -*- coding: utf-8 -*-
import os
from setuptools import setup


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


def get_version():
    """Get version from the package without actually importing it."""
    init = read('cabbagok/__init__.py')
    for line in init.split('\n'):
        if line.startswith('__version__'):
            return eval(line.split('=')[1])


setup(
    name='cabbagok',
    version=get_version(),
    description='asyncio-based AMQP client and server for RPC.',
    packages=['cabbagok'],
    url='https://github.com/whiteapfel/cabbagok/',
    download_url='https://pypi.org/project/cabbagok/',
    install_requires=read('requirements.txt'),
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    maintainer='WhiteApfel',
    maintainer_email='white@pfel.ru',
    license='Mozilla Public License 2.0',
    python_requires='>=3.11',
    zip_safe=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ],
)
