#!/usr/bin/env python
# -*- coding: latin-1 -*-
# #
# Copyright 2014-2014 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
# #
"""
vsc-filesystems-quota base distribution setup.py

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""
import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag

PACKAGE = {
    'version': '0.9.3',
    'author': [ag],
    'maintainer': [ag],
    'excluded_pkgs_rpm': ['vsc', 'vsc.filesystem', 'vsc.filesystem.quota'],
    'install_requires': [
        'vsc-accountpage-clients',
        'vsc-administration >= 0.35',
        'vsc-base >= 1.6.6',
        'vsc-config >= 1.10',
        'vsc-filesystems >= 0.28',
        'vsc-utils >= 1.4.6',
    ],
    'tests_require': ['mock'],
    'dependency_links': [
        "git+https://github.com/hpcugent/vsc-utils.git#egg=vsc-utils-1.8.2",
        "git+https://github.com/hpcugent/vsc-filesystems.git#egg=vsc-filesystems-0.30.1",
        "git+ssh://github.com/hpcugent/vsc-accountpage-clients.git#egg=vsc-accountpage-clients-0.7",
        "git+ssh://github.com/hpcugent/vsc-administration.git#egg=vsc-administration-0.35",
    ],


}

if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
