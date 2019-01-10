#
# Copyright 2015-2018 Ghent University
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
Helper functions for all things quota related.

@author: Andy Georges (Ghent University)
"""

import inspect
import logging
import pwd
import re
import socket
import time

from collections import namedtuple

from vsc.config.base import GENT_VO_PREFIX, GENT_VO_SHARED_PREFIX, STORAGE_SHARED_SUFFIX, GENT
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.utils.mail import VscMail

GPFS_GRACE_REGEX = re.compile(
    r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)"
)

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)

QUOTA_USER_KIND = 'user'
QUOTA_VO_KIND = 'vo'


class QuotaException(Exception):
    pass


InodeCritical = namedtuple("InodeCritical", ['used', 'allocated', 'maxinodes'])


CRITICAL_INODE_COUNT_MESSAGE = """
Dear HPC admins,

The following filesets will be running out of inodes soon (or may already have run out).

%(fileset_info)s

Kind regards,
Your friendly inode-watching script
"""


class DjangoPusher(object):
    """Context manager for pushing stuff to django"""

    def __init__(self, storage_name, client, kind, dry_run):
        self.storage_name = storage_name
        self.storage_name_shared = storage_name + "_SHARED"
        self.client = client
        self.kind = kind
        self.dry_run = dry_run

        self.count = {
            self.storage_name: 0,
            self.storage_name_shared: 0
        }

        self.payload = {
            self.storage_name: [],
            self.storage_name_shared: []
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.payload[self.storage_name]:
            self._push(self.storage_name, self.payload[self.storage_name])
        if self.payload[self.storage_name_shared]:
            self._push(self.storage_name_shared, self.payload[self.storage_name_shared])

        if exc_type is not None:
            logging.error("Received exception %s in DjangoPusher: %s", exc_type, exc_value)
            return False

        return True

    def push(self, storage_name, payload):
        self.payload[storage_name].append(payload)
        self.count[storage_name] += 1

        if self.count[storage_name] > 100:
            self._push(storage_name, self.payload[storage_name])
            self.count[storage_name] = 0
            self.payload[storage_name] = []

    def _push(self, storage_name, payload):
        """Does the actual pushing to the REST API"""

        if self.dry_run:
            logging.info("Would push payload to account web app: %s" % (payload,))
        else:
            try:
                cl = self.client.usage.storage[storage_name]
                if self.kind == QUOTA_USER_KIND:
                    logging.debug("Pushing user payload to account web app: %s", payload)
                    cl = cl.user
                elif self.kind == QUOTA_VO_KIND:
                    logging.debug("Pushing vo payload to account web app: %s", payload)
                    cl = cl.vo
                else:
                    logging.error("Unknown quota kind, not pushing any quota to the account page")
                    return
                cl.size.put(body=payload)  # if all is well, there's nothing returned except (200, empty string)
            except Exception:
                logging.error("Could not store quota info in account web app")
                raise


def process_user_quota(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, dry_run=False):
    """
    Wrapper around the new function to keep the old behaviour intact.
    """
    del filesystem
    del gpfs

    exceeding_users = []
    path_template = storage.path_templates[GENT][storage_name]

    push_user_quota_to_django(user_map, storage_name, path_template, quota_map, client, dry_run)

    for (user_id, quota) in quota_map.items():
        user_name = user_map.get(int(user_id), None)
        if user_name and user_name.startswith('vsc4') and quota.exceeds():
            exceeding_users.append((user_name, quota))

    return exceeding_users


def process_user_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, user_map, client,
                                      store_cache=False, dry_run=False):
    """
    Store the information in the user directories and in the account page.

    Deprecated. Does nothing anymore.
    """
    del storage
    del gpfs
    del storage_name
    del filesystem
    del quota_map
    del user_map
    del client
    del store_cache
    del dry_run
    logging.warning("The %s function has been deprecated and should not longer be used." % inspect.stack()[0][3])
    pass


def get_mmrepquota_maps(quota_map, storage, filesystem, filesets,
                        replication_factor=1):
    """Obtain the quota information.

    This function uses vsc.filesystem.gpfs.GpfsOperations to obtain
    quota information for all filesystems known to the storage.

    The returned dictionaries contain all information on a per user
    and per fileset basis for the given filesystem. Users with multiple
    quota settings across different filesets are processed correctly.

    Returns { "USR": user dictionary, "FILESET": fileset dictionary}.

    @type replication_factor: int, describing the number of copies the FS holds for each file
    @type metadata_replication_factor: int, describing the number of copies the FS metadata holds for each file
    """
    user_map = {}
    fs_map = {}

    timestamp = int(time.time())

    logging.info("ordering USR quota for storage %s", storage)
    # Iterate over a list of named tuples -- GpfsQuota
    for (user, gpfs_quota) in quota_map['USR'].items():
        user_quota = user_map.get(user, QuotaUser(storage, filesystem, user))
        user_map[user] = _update_quota_entity(
            filesets,
            user_quota,
            filesystem,
            gpfs_quota,
            timestamp,
            replication_factor
        )

    logging.info("ordering FILESET quota for storage %s", storage)
    # Iterate over a list of named tuples -- GpfsQuota
    for (fileset, gpfs_quota) in quota_map['FILESET'].items():
        fileset_quota = fs_map.get(fileset, QuotaFileset(storage, filesystem, fileset))
        fs_map[fileset] = _update_quota_entity(
            filesets,
            fileset_quota,
            filesystem,
            gpfs_quota,
            timestamp,
            replication_factor
        )

    return {"USR": user_map, "FILESET": fs_map}


def determine_grace_period(grace_string):
    grace = GPFS_GRACE_REGEX.search(grace_string)
    nograce = GPFS_NOGRACE_REGEX.search(grace_string)

    if nograce:
        expired = (False, None)
    elif grace:
        grace = grace.groupdict()
        grace_time = 0
        if grace['days']:
            grace_time = int(grace['days']) * 86400
        elif grace['hours']:
            grace_time = int(grace['hours']) * 3600
        elif grace['minutes']:
            grace_time = int(grace['minutes']) * 60
        elif grace['expired']:
            grace_time = 0
        else:
            logging.error("Unprocessed grace groupdict %s (from string %s).",
                          grace, grace_string)
            raise QuotaException("Cannot process grace time string")
        expired = (True, grace_time)
    else:
        logging.error("Unknown grace string %s.", grace_string)
        raise QuotaException("Cannot process grace information (%s)" % grace_string)

    return expired


def _update_quota_entity(filesets, entity, filesystem, gpfs_quotas, timestamp, replication_factor=1):
    """
    Update the quota information for an entity (user or fileset).

    @type filesets: string
    @type entity: QuotaEntity instance
    @type filesystem: string
    @type gpfs_quota: list of GpfsQuota namedtuple instances
    @type timestamp: a timestamp, duh. an integer
    @type replication_factor: int, describing the number of copies the FS holds for each file
    """
    for quota in gpfs_quotas:
        logging.debug("gpfs_quota = %s" % (str(quota)))

        block_expired = determine_grace_period(quota.blockGrace)
        files_expired = determine_grace_period(quota.filesGrace)

        if quota.filesetname:
            fileset_name = filesets[filesystem][quota.filesetname]['filesetName']
        else:
            fileset_name = None

        logging.debug("The fileset name is %s (filesystem %s); blockgrace %s to expired %s",
                      fileset_name, filesystem, quota.blockGrace, block_expired)

        # XXX: We do NOT divide by the metatadata_replication_factor (yet), since we do not
        #      set the inode quota through the account page. As such, we need to have the exact
        #      usage available for the user -- this is the same data reported in ES by gpfsbeat.
        entity.update(fileset=fileset_name,
                      used=int(quota.blockUsage) // replication_factor,
                      soft=int(quota.blockQuota) // replication_factor,
                      hard=int(quota.blockLimit) // replication_factor,
                      doubt=int(quota.blockInDoubt) // replication_factor,
                      expired=block_expired,
                      files_used=int(quota.filesUsage),
                      files_soft=int(quota.filesQuota),
                      files_hard=int(quota.filesLimit),
                      files_doubt=int(quota.filesInDoubt),
                      files_expired=files_expired,
                      timestamp=timestamp)

    return entity


def process_fileset_quota(storage, gpfs, storage_name, filesystem, quota_map, client, dry_run=False):
    """wrapper around the new function to keep the old behaviour intact"""
    del storage
    filesets = gpfs.list_filesets()
    exceeding_filesets = []

    push_vo_quota_to_django(storage_name, quota_map, client, dry_run, filesets, filesystem)

    logging.debug("filesets = %s", filesets)

    for (fileset, quota) in quota_map.items():
        fileset_name = filesets[filesystem][fileset]['filesetName']
        logging.debug("Fileset %s quota: %s", fileset_name, quota)

        if quota.exceeds():
            exceeding_filesets.append((fileset_name, quota))

    return exceeding_filesets


def process_fileset_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, client,
                                         store_cache=False, dry_run=False):
    """Store the quota information in the filesets.

    Deprecated. Does nothing anymore.
    """
    del storage
    del gpfs
    del storage_name
    del filesystem
    del quota_map
    del client
    del store_cache
    del dry_run
    logging.warning("The %s function has been deprecated and should not longer be used." % inspect.stack()[0][3])
    pass


def push_user_quota_to_django(user_map, storage_name, path_template, quota_map, client, dry_run=False):
    """
    Upload the quota information to the account page, so it can be displayed for the users in the web application.
    """
    logging.info("Logging user quota to account page")
    logging.debug("Considering the following quota items for pushing: %s", quota_map)

    with DjangoPusher(storage_name, client, QUOTA_USER_KIND, dry_run) as pusher:
        for (user_id, quota) in quota_map.items():

            user_name = user_map.get(int(user_id), None)
            if not user_name or not user_name.startswith('vsc4'):
                continue

            sanitize_quota_information(path_template['user'](user_name)[1], quota)

            for (fileset, quota_) in quota.quota_map.items():

                params = {
                    "fileset": fileset,
                    "user": user_name,
                    "used": quota_.used,
                    "soft": quota_.soft,
                    "hard": quota_.hard,
                    "doubt": quota_.doubt,
                    "expired": quota_.expired[0],
                    "remaining": quota_.expired[1] or 0,  # seconds
                    "files_used": quota_.files_used,
                    "files_soft": quota_.files_soft,
                    "files_hard": quota_.files_hard,
                    "files_doubt": quota_.files_doubt,
                    "files_expired": quota_.files_expired[0],
                    "files_remaining": quota_.files_expired[1],  # seconds
                }
                pusher.push(storage_name, params)


def push_vo_quota_to_django(storage_name, quota_map, client, dry_run=False, filesets=None, filesystem=None):
    """
    Upload the VO usage information to the account page, so it can be displayed in the web interface.
    """
    logging.info("Logging VO quota to account page")
    logging.debug("Considering the following quota items for pushing: %s", quota_map)

    with DjangoPusher(storage_name, client, QUOTA_VO_KIND, dry_run) as pusher:

        for (fileset, quota) in quota_map.items():
            fileset_name = filesets[filesystem][fileset]['filesetName']
            logging.debug("Fileset %s quota: %s", fileset_name, quota)

            if not fileset_name.startswith(GENT_VO_PREFIX):
                continue

            if fileset_name.startswith(GENT_VO_SHARED_PREFIX):
                derived_vo_name = fileset_name.replace(GENT_VO_SHARED_PREFIX, GENT_VO_PREFIX)
                derived_storage_name = storage_name + STORAGE_SHARED_SUFFIX
            else:
                derived_vo_name = fileset_name
                derived_storage_name = storage_name

            for (fileset_, quota_) in quota.quota_map.items():

                params = {
                    "vo": derived_vo_name,
                    "fileset": fileset_,
                    "used": quota_.used,
                    "soft": quota_.soft,
                    "hard": quota_.hard,
                    "doubt": quota_.doubt,
                    "expired": quota_.expired[0],
                    "remaining": quota_.expired[1] or 0,  # seconds
                    "files_used": quota_.files_used,
                    "files_soft": quota_.files_soft,
                    "files_hard": quota_.files_hard,
                    "files_doubt": quota_.files_doubt,
                    "files_expired": quota_.files_expired[0],
                    "files_remaining": quota_.files_expired[1], # seconds
                }
                pusher.push(derived_storage_name, params)


def sanitize_quota_information(fileset_name, quota):
    """Sanitize the information that is store at the user's side.

    There should be _no_ information regarding filesets besides:
        - vscixy (note that on muk, each user had his own fileset, so vsc1, vsc2, and vsc3 prefixes are possible)
        - gvo*
        - project
    """
    for fileset in quota.quota_map.keys():
        if not fileset.startswith('vsc') and \
           not fileset.startswith(GENT_VO_PREFIX) and \
           not fileset.startswith(GENT_VO_SHARED_PREFIX) and \
           not fileset.startswith(fileset_name):
            quota.quota_map.pop(fileset)


def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = pwd.getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d


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

        if maxinodes > 0 and used > threshold * maxinodes:
            critical_filesets[fs_info['filesetName']] = InodeCritical(used=used, allocated=allocated,
                                                                      maxinodes=maxinodes)

    return critical_filesets


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
