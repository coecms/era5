#!/usr/bin/env python
#
# Uses Python Build Reasonableness https://docs.openstack.org/developer/pbr/
# Add configuration to `setup.cfg`

from setuptools import setup

setup(
        setup_requires=['pbr>=1.9', 'setuptools>=17.1'],
        pbr=True,
        )
