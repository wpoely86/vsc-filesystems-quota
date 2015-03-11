#!/usr/bin/env python
#
# Copyright 2013-2016 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.ugent.be/hpcugent/vsc-filesystems-quota
#
# vsc-filesystems-quota is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-filesystems-quota is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-filesystems-quota. If not, see <http://www.gnu.org/licenses/>.
#
"""
This script stores the inode usage information for the various mounted GPFS filesystems
in a zip file, named by date and filesystem.

@author Andy Georges (Ghent University)
"""
import gzip
import json
import os
import socket
import sys
import time


from vsc.filesystem.gpfs import GpfsOperations
from vsc.utils import fancylogger
from vsc.utils.mail import VscMail
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
INODE_LOG_ZIP_PATH = '/var/log/quota/inode-zips'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

INODE_STORE_LOG_CRITICAL = 1


from vsc.filesystem.quota.process import InodeCritical, process_inodes_information


def mail_admins(critical_filesets, dry_run):
    """Send email to the HPC admin about the inodes running out soonish."""
    mail = VscMail(mail_host="smtp.ugent.be")

    message = """
Dear HPC admins,

The following filesets will be running out of inodes soon (or may already have run out).

%(fileset_info)s

Kind regards,
Your friendly inode-watching script
"""
    fileset_info = []
    for (fs_name, fs_info) in critical_filesets.items():
        for (fileset_name, inode_info) in fs_info.items():
            fileset_info.append("%s - %s: used %d (%d%%) of max %d [allocated: %d]" % (fs_name,
                                                                 fileset_name,
                                                                 inode_info.used,
                                                                 int(inode_info.used * 100 / inode_info.maxinodes),
                                                                 inode_info.maxinodes,
                                                                 inode_info.allocated))

    message = message % ({'fileset_info': "\n".join(fileset_info)})

    if dry_run:
        logger.info("Would have sent this message: %s" % (message,))
    else:
        mail.sendTextMail(mail_to="hpc-admin@lists.ugent.be",
                          mail_from="hpc-admin@lists.ugent.be",
                          reply_to="hpc-admin@lists.ugent.be",
                          mail_subject="Inode space(s) running out on %s" % (socket.gethostname()),
                          message=message)


def main():
    """The main."""

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', INODE_LOG_ZIP_PATH),
    }

    opts = ExtendedSimpleOption(options)

    stats = {}

    try:
        gpfs = GpfsOperations()
        filesets = gpfs.list_filesets()
        quota = gpfs.list_quota()

        if not os.path.exists(opts.options.location):
            os.makedirs(opts.options.location, 0755)

        critical_filesets = dict()

        for filesystem in filesets:
            stats["%s_inodes_log_critical" % (filesystem,)] = INODE_STORE_LOG_CRITICAL
            try:
                filename = "gpfs_inodes_%s_%s.gz" % (time.strftime("%Y%m%d-%H:%M"), filesystem)
                path = os.path.join(opts.options.location, filename)
                zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                zipfile.write(json.dumps(filesets[filesystem]))
                zipfile.close()
                stats["%s_inodes_log" % (filesystem,)] = 0
                logger.info("Stored inodes information for FS %s" % (filesystem))

                cfs = process_inodes_information(filesets[filesystem], quota[filesystem]['FILESET'], threshold=0.9)
                logger.info("Processed inodes information for filesystem %s" % (filesystem,))
                if cfs:
                    critical_filesets[filesystem] = cfs
                    logger.info("Filesystem %s has at least %d filesets reaching the limit" % (filesystem, len(cfs)))

            except Exception:
                stats["%s_inodes_log" % (filesystem,)] = 1
                logger.exception("Failed storing inodes information for FS %s" % (filesystem))

        logger.info("Critical filesets: %s" % (critical_filesets,))

        if critical_filesets:
            mail_admins(critical_filesets, opts.options.dry_run)

    except Exception:
        logger.exception("Failure obtaining GPFS inodes")
        opts.critical("Failure to obtain GPFS inodes information")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Logged GPFS inodes", stats)

if __name__ == '__main__':
    main()
