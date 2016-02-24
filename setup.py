#!/usr/bin/env python
from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt", session=False)

reqs = [str(ir.req) for ir in install_reqs]

from distutils.core import setup

setup(name='alooma',
      version='0.1.22',
      description='Alooma python API',
      author='Yonatan Kiron',
      author_email='yonatan@alooma.io',
      packages=['alooma'],
      install_requires=reqs,
      keywords=['alooma']
)
