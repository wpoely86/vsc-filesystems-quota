#
# Copyright 2015-2016 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
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

import logging
import os
import pwd
import re
import time

from string import Template
from urllib2 import HTTPError

from vsc.administration.user import VscTier2AccountpageUser
from vsc.administration.vo import VscTier2AccountpageVo
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.mail import VscMail

GPFS_GRACE_REGEX = re.compile(r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)")

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)

QUOTA_USER_KIND = 'user'
QUOTA_VO_KIND = 'vo'

# log setup
logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

QUOTA_NOTIFICATION_CACHE_THRESHOLD = 7 * 86400

QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE = Template("""
Dear $user_name


We have noticed that you have exceeded your quota on the VSC storage,
more in particular: $storage_name

As you may know, this may have a significant impact on the jobs you
can run on the various clusters.

Please clean up any files you no longer require.

Should you need more storage, you can use your VO storage.
If you are not a member of a VO, please consider joining one or request
a VO to be created for your research group. If your VO storage is full,
please ask its moderator ask to increase the quota.

Also, it is recommended to clear scratch storage and move data you wish
to keep to $$VSC_DATA or $$VSC_DATA_VO/$USER. It is paramount that scratch
space remains temporary storage for running (multi-node) jobs as it is
accessible faster than both $$VSC_HOME and $$VSC_DATA.

At this point on $time, your personal usage is the following:
$quota_info


Kind regards,
The UGent HPC team
""")


VO_QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE = Template("""
Dear $user_name


We have noticed that the VO ($vo_name) you moderate has exceeded its quota on the VSC storage,
more in particular: $$$storage_name

As you may know, this may have a significant impact on the jobs the VO members
can run on the various clusters.

Please clean up any files that are no longer required.

Should you need more storage, you can reply to this mail and ask for
the quota to be increased. Please motivate your request adequately.

Also, it is recommended to have your VO members clear scratch storage and move data they wish
to keep to $$VSC_DATA or $$VSC_DATA_VO/$USER. It is paramount that scratch
space remains temporary storage for running (multi-node) jobs as it is
accessible faster than both $$VSC_HOME and $$VSC_DATA.

At this point on $time, the VO  usage is the following:
$quota_info

You can check your quota by running the show_quota command on the login nodes or
visit the VSC account page at https://account.vscentrum.be/. Note that the information
there is cached and may not show the most recent information.

Kind regards,
The UGent HPC team
""")

IGNORED_ACCOUNTS = ('vsc40024',)



def process_user_quota(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, dry_run=False):
    """wrapper around the new function to keep the old behaviour intact"""
    process_user_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, False, dry_run)
    process_user_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, True, dry_run)


def process_user_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, store_cache, dry_run=False):
    """Store the information in the user directories.
    """
    exceeding_users = []
    login_mount_point = storage[storage_name].login_mount_point
    gpfs_mount_point = storage[storage_name].gpfs_mount_point
    path_template = storage.path_templates[storage_name]

    if store_cache:
        push_user_quota_to_django(user_map, storage_name, path_template, quota_map, client, dry_run)

    for (user_id, quota) in quota_map.items():

        user_name = user_map.get(int(user_id), None)

        if user_name and user_name.startswith('vsc4'):
            if quota.exceeds():
                exceeding_users.append((user_name, quota))

            if not store_cache:
                continue

            if user_name in IGNORED_ACCOUNTS:
                logger.info("Not processing %s", user_name)
                continue
            try:
                user = VscTier2AccountpageUser(user_name, rest_client=client)
            except HTTPError:
                logger.warning("Cannot find an account for user %s", user_name)
                continue
            except AttributeError:
                logger.warning("Cannot store quota infor for account %s", user_name)
                continue

            logger.debug("Checking quota for user %s with ID %s", user_name, user_id)
            logger.debug("User %s quota: %s", user, quota)

            path = user._get_path(storage_name)

            logger.debug("path for storing quota info would be %s", path)

            # FIXME: We need some better way to address this
            # Right now, we replace the nfs mount prefix which the symlink points to
            # with the gpfs mount point. this is a workaround until we resolve the
            # symlink problem once we take new default scratch into production
            if gpfs.is_symlink(path):
                target = os.path.realpath(path)
                logger.debug("path is a symlink, target is %s", target)
                logger.debug("login_mount_point for %s is %s", storage_name, login_mount_point)
                if target.startswith(login_mount_point):
                    new_path = target.replace(login_mount_point, gpfs_mount_point, 1)
                    logger.info("Found a symlinked path %s to the nfs mount point %s. Replaced with %s" %
                                (path, login_mount_point, gpfs_mount_point))
                else:
                    logger.warning("Unable to store quota information for %s on %s; symlink cannot be resolved properly",
                                   user_name, storage_name)
            else:
                new_path = path

            path_stat = os.stat(new_path)
            filename = os.path.join(new_path, ".quota_user.json.gz")

            sanitize_quota_information(path_template['user'][0], quota)

            if dry_run:
                logger.info("Dry run: would update cache for %s at %s with %s",
                            storage_name, new_path, "%s", quota)
                logger.info("Dry run: would chmod 640 %s", filename)
                logger.info("Dry run: would chown %s to %s %s", filename, path_stat.st_uid, path_stat.st_gid)
            else:
                cache = FileCache(filename, False)
                cache.update(key="quota", data=quota, threshold=0)
                cache.update(key="storage_name", data=storage_name, threshold=0)
                cache.close()

                gpfs.ignorerealpathmismatch = True
                gpfs.chmod(0640, filename)
                gpfs.chown(path_stat.st_uid, path_stat.st_uid, filename)
                gpfs.ignorerealpathmismatch = False

            logger.info("Stored user %s quota for storage %s at %s" % (user_name, storage_name, filename))

    return exceeding_users


def get_mmrepquota_maps(quota_map, storage, filesystem, filesets, replication_factor=1):
    """Obtain the quota information.

    This function uses vsc.filesystem.gpfs.GpfsOperations to obtain
    quota information for all filesystems known to the storage.

    The returned dictionaries contain all information on a per user
    and per fileset basis for the given filesystem. Users with multiple
    quota settings across different filesets are processed correctly.

    Returns { "USR": user dictionary, "FILESET": fileset dictionary}.

    @type replication_factor: int, describing the number of copies the FS holds for each file
    """
    user_map = {}
    fs_map = {}

    timestamp = int(time.time())

    logger.info("ordering USR quota for storage %s" % (storage))
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

    logger.info("ordering FILESET quota for storage %s" % (storage))
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
        logger.debug("gpfs_quota = %s" % (str(quota)))
        grace = GPFS_GRACE_REGEX.search(quota.blockGrace)
        nograce = GPFS_NOGRACE_REGEX.search(quota.blockGrace)

        if nograce:
            expired = (False, None)
        elif grace:
            grace = grace.groupdict()
            if grace.get('days', None):
                expired = (True, int(grace['days']) * 86400)
            elif grace.get('hours', None):
                expired = (True, int(grace['hours']) * 3600)
            elif grace.get('minutes', None):
                expired = (True, int(grace['minutes']) * 60)
            elif grace.get('expired', None):
                expired = (True, 0)
            else:
                logger.raiseException("Unprocessed grace groupdict %s (from string %s)." %
                                      (grace, quota.blockGrace))
        else:
            logger.raiseException("Unknown grace string %s." % quota.blockGrace)

        if quota.filesetname:
            fileset_name = filesets[filesystem][quota.filesetname]['filesetName']
        else:
            fileset_name = None
        logger.debug("The fileset name is %s (filesystem %s); blockgrace %s to expired %s" %
                     (fileset_name, filesystem, quota.blockGrace, expired))
        entity.update(fileset_name,
                      int(quota.blockUsage) // replication_factor,
                      int(quota.blockQuota) // replication_factor,
                      int(quota.blockLimit) // replication_factor,
                      int(quota.blockInDoubt) // replication_factor,
                      expired,
                      timestamp)

    return entity


def process_fileset_quota(storage, gpfs, storage_name, filesystem, quota_map, client, dry_run=False):
    """wrapper around the new function to keep the old behaviour intact"""
    process_fileset_quota(storage, gpfs, storage_name, filesystem, quota_map, client, True, dry_run)
    process_fileset_quota(storage, gpfs, storage_name, filesystem, quota_map, client, False, dry_run)


def process_fileset_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, client, store_cache, dry_run=False):
    """Store the quota information in the filesets.
    """

    filesets = gpfs.list_filesets()
    exceeding_filesets = []

    if store_cache:
        push_vo_quota_to_django(storage_name, quota_map, client, dry_run)

    logger.info("filesets = %s", filesets)

    for (fileset, quota) in quota_map.items():
        fileset_name = filesets[filesystem][fileset]['filesetName']
        logger.debug("Fileset %s quota: %s", fileset_name, quota)

        if quota.exceeds():
            exceeding_filesets.append((fileset_name, quota))

        if not store_cache:
            continue

        path = filesets[filesystem][fileset]['path']
        filename = os.path.join(path, ".quota_fileset.json.gz")
        path_stat = os.stat(path)

        if dry_run:
            logger.info("Dry run: would update cache for %s at %s with %s", storage_name, path, "%s" % (quota,))
            logger.info("Dry run: would chmod 640 %s", filename)
            logger.info("Dry run: would chown %s to %s %s", filename, path_stat.st_uid, path_stat.st_gid)
        else:
            # TODO: This should somehow be some atomic operation.
            cache = FileCache(filename, False)
            cache.update(key="quota", data=quota, threshold=0)
            cache.update(key="storage_name", data=storage_name, threshold=0)
            cache.close()

            gpfs.chmod(0640, filename)
            gpfs.chown(path_stat.st_uid, path_stat.st_gid, filename)

        logger.info("Stored fileset %s [%s] quota for storage %s at %s", fileset, fileset_name, storage, filename)

    return exceeding_filesets


def push_user_quota_to_django(user_map, storage_name, path_template, quota_map, client, dry_run=False):
    """
    Upload the quota information to the django database, so it can be displayed for the users in the web application.
    """

    payload = []
    count = 0

    logger.info("Logging user quota to account page")
    logger.debug("Considering the following quota items for pushing: %s", quota_map)

    for (user_id, quota) in quota_map.items():

        user_name = user_map.get(int(user_id), None)
        if not user_name or not user_name.startswith('vsc4'):
            continue

        sanitize_quota_information(path_template['user'][0], quota)

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
            }
            payload.append(params)
            count += 1

            if count > 100:
                push_quota_to_django(storage_name, QUOTA_USER_KIND, client, payload, dry_run)
                count = 0
                payload = []

    if payload:
        push_quota_to_django(storage_name, QUOTA_USER_KIND, client, payload, dry_run)


def push_vo_quota_to_django(storage_name, quota_map, client, payload, dry_run=False):
    pass


def push_quota_to_django(storage_name, kind, client, payload, dry_run=False):

    if dry_run:
        logger.info("Would push payload to account web app: %s" % (payload,))
    else:
        try:
            cl = client.usage.storage[storage_name]
            if kind == QUOTA_USER_KIND:
                logger.debug("Pushing user payload to account web app: %s", payload)
                cl = cl.user
            elif kind == QUOTA_VO_KIND:
                logger.debug("Pushing vo payload to account web app: %s", payload)
                cl = cl.vo
            else:
                logger.error("Unknown quota kind, not pushing any quota to the account page")
                return
            cl.size.put(body=payload)  # if all is well, there's nothing returned except (200, empty string)
        except Exception:
            logger.raiseException("Could not store quota info in account web app")


def sanitize_quota_information(fileset_name, quota):
    """Sanitize the information that is store at the user's side.

    There should be _no_ information regarding filesets besides:
        - vsc4xy
        - gvo*
    """
    for (fileset, value) in quota.quota_map.items():
        if not fileset.startswith('vsc4') and not fileset.startswith('gvo') and not fileset.startswith(fileset_name):
            quota.quota_map.pop(fileset)


def notify(storage_name, item, quota, client, dry_run=False):
    """Send out the notification"""
    mail = VscMail(mail_host="smtp.ugent.be")
    if isinstance(item, tuple):
        item = item[0]
    if item.startswith("gvo"):  # VOs
        vo = VscTier2AccountpageVo(item, rest_client=client)
        for user in [VscTier2AccountpageUser(m, rest_client=client) for m in vo.vo.moderators]:
            message = VO_QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(user_name=user.person.gecos,
                                                                           vo_name=item,
                                                                           storage_name=storage_name,
                                                                           quota_info="%s" % (quota,),
                                                                           time=time.ctime())
            if dry_run:
                logger.info("Dry-run, would send the following message: %s" % (message,))
            else:
                mail.sendTextMail(mail_to=user.account.email,
                                  mail_from="hpc@ugent.be",
                                  reply_to="hpc@ugent.be",
                                  mail_subject="Quota on %s exceeded" % (storage_name,),
                                  message=message)
            logger.info("notification: recipient %s storage %s quota_string %s" %
                        (user.account.vsc_id, storage_name, "%s" % (quota,)))

    elif item.startswith("gpr"):  # projects
        pass
    elif item.startswith("vsc"):  # users
        logging.info("notifying VSC user %s", item)
        user = VscTier2AccountpageUser(item, rest_client=client)

        exceeding_filesets = [fs for (fs, q) in quota.quota_map.items() if q.expired[0]]
        storage_names = []
        if [ef for ef in exceeding_filesets if not ef.startswith("gvo")]:
            storage_names.append(storage_name)
        if [ef for ef in exceeding_filesets if ef.startswith("gvo")]:
            storage_names.append(storage_name + "_VO")
        storage_names = ", ".join(["$" + sn for sn in storage_names])

        message = QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(user_name=user.person.gecos,
                                                                    storage_name=storage_names,
                                                                    quota_info="%s" % (quota,),
                                                                    time=time.ctime())
        if dry_run:
            logger.info("Dry-run, would send the following message: %s" % (message,))
        else:
            mail.sendTextMail(mail_to=user.account.email,
                              mail_from="hpc@ugent.be",
                              reply_to="hpc@ugent.be",
                              mail_subject="Quota on %s exceeded" % (storage_name,),
                              message=message)
        logger.info("notification: recipient %s storage %s quota_string %s" %
                    (user.account.vsc_id, storage_name, "%s" % (quota,)))
    else:
        logger.error("Should send a mail, but cannot process item %s" % (item,))



def notify_exceeding_items(gpfs, storage, filesystem, exceeding_items, target, client, dry_run=False):
    """Send out notification to the fileset owners.

    - if the fileset belongs to a VO: the VO moderator
    - if the fileset belongs to a project: the project moderator
    - if the fileset belongs to a user: the user

    The information is cached. The mail is sent in the following cases:
        - the excession is new
        - the excession occurred more than 7 days ago and stayed in the cache. In this case, the cache is updated as
          to avoid sending outdated mails repeatedly.
    """
    cache_path = os.path.join(
        gpfs.list_filesystems()[filesystem]['defaultMountPoint'],
        ".quota_%s_cache.json.gz" % (target)
    )
    cache = FileCache(cache_path, True)  # we retain the old data

    logging.info("Processing %d exceeding items" % (len(exceeding_items)))

    updated_cache = False
    for (item, quota) in exceeding_items:
        updated = cache.update(item, quota, QUOTA_NOTIFICATION_CACHE_THRESHOLD)
        logging.info("Storage %s: cache entry for %s was updated: %s" % (storage, item, updated))
        if updated:
            notify(storage, item, quota, client, dry_run)
        updated_cache = updated_cache or updated

    if not dry_run and updated_cache:
        cache.close()

    if dry_run:
        logging.info("dry-run: not saving any cache")


def notify_exceeding_filesets(**kwargs):
    """Notification for filesets that have exceeded their quota."""

    kwargs['target'] = 'filesets'
    notify_exceeding_items(**kwargs)


def notify_exceeding_users(**kwargs):
    """Notification for users who have exceeded their quota."""
    kwargs['target'] = 'users'
    notify_exceeding_items(**kwargs)


def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = pwd.getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d
