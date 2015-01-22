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


def remove_bdist_rpm_source_file():
    """List of files to remove from the (source) RPM."""
    return []


shared_setup.remove_extra_bdist_rpm_files = remove_bdist_rpm_source_file
shared_setup.SHARED_TARGET.update({
    'url': 'https://github.ugent.be/hpcugent/vsc-filesystems-quota',
    'download_url': 'https://github.ugent.be/hpcugent/vsc-filesystems-quota'
})


PACKAGE = {
    'name': 'vsc-filesystems-quota',
    'version': '0.3',
    'author': [ag],
    'maintainer': [ag],
    'namespace_packages': [],
    'scripts': ['bin/quota_log.py',
                'bin/inode_log.py',
                'bin/dquota.py',
                'bin/my_show_quota.sh',
                'bin/show_quota.py',
                ],
    'install_requires': [
        'vsc-base >= 1.6.6',
        'vsc-config >= 1.10',
        'vsc-filesystems >= 0.28',
        'vsc-utils >= 1.4.6',
    ],
}

if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
