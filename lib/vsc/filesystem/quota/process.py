#
# Copyright 2015-2015 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
"""
A collection of data types and functions used by the quota checking scripts.

@author: Andy Georges
"""
from collections import namedtuple


InodeCritical = namedtuple('InodeCritical', 'used, allocated, maxinodes')


def process_inodes_information(filesets, quota, threshold=0.9):
    """
    Determines which filesets have reached a critical inode limit.

    For this it uses the inode quota information passed in the quota argument and compares this with the maximum number
    of inodes that can be allocated for the given fileset. The default threshold is placed at 90%.

    @returns: dict with (filesetname, InodeCritical) key-value pairs
    """
    critical_filesets = dict()

    for (fs_key, fs_info) in filesets.items():
        allocated = int(fs_info['allocInodes'])
        maxinodes = int(fs_info['maxInodes'])
        used = int(quota[fs_key][0].filesUsage)

        if used > 0.9 * maxinodes:
            critical_filesets[fs_info['filesetName']] = InodeCritical(used=used, allocated=allocated, maxinodes=maxinodes)

    return critical_filesets



