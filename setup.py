#!/usr/bin/python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'FusionPy, a Python adaptation of the Lucidworks Fusion REST API',
    'author': 'Jim Scarborough',
    'url': 'https://github.com/ke4roh/fusionpy',
#   'download_url': 'Where to download it.',
    'author_email': 'jscarbor@redhat.com',
    'version': '0.1',
    'install_requires': ['nose','tqdm','urllib3', 'stubserver'],
    'packages': ['fusionpy'],
    'scripts': [],
    'name': 'fusionpy'
}

setup(**config)
