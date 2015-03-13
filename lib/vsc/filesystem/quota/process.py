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
import logging
import socket

from collections import namedtuple

from vsc.utils.mail import VscMail

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

CRITICAL_INODE_COUNT_MESSAGE ="""
Dear HPC admins,

The following filesets will be running out of inodes soon (or may already have run out).

%(fileset_info)s

Kind regards,
Your friendly inode-watching script
"""


def mail_admins(critical_filesets, dry_run=True):
    """Send email to the HPC admin about the inodes running out soonish."""
    mail = VscMail(mail_host="smtp.ugent.be")

    message = CRITICAL_INODE_COUNT_MESSAGE
    fileset_info = []
    for (fs_name, fs_info) in critical_filesets.items():
        for (fileset_name, inode_info) in fs_info.items():
            fileset_info.append("%s - %s: used %d (%d%%) of max %d [allocated: %d]" %
                                (fs_name,
                                 fileset_name,
                                 inode_info.used,
                                 int(inode_info.used * 100 / inode_info.maxinodes),
                                 inode_info.maxinodes,
                                 inode_info.allocated))

    message = message % ({'fileset_info': "\n".join(fileset_info)})

    if dry_run:
        logging.info("Would have sent this message: %s" % (message,))
    else:
        mail.sendTextMail(mail_to="hpc-admin@lists.ugent.be",
                          mail_from="hpc-admin@lists.ugent.be",
                          reply_to="hpc-admin@lists.ugent.be",
                          mail_subject="Inode space(s) running out on %s" % (socket.gethostname()),
                          message=message)
