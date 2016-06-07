#!/usr/bin/env python
#
# Copyright 2012-2015 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
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
Tests for all helper functions in vsc.filesystems.quota.tools.

@author: Andy Georges (Ghent University)
"""
import mock
import time

import vsc.filesystem.quota.tools as tools

from vsc.accountpage.wrappers import mkVscAccount, mkVo, mkVscAccountPerson
from vsc.config.base import VSC_HOME, VSC_DATA
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset
from vsc.install.testing import TestCase
from vsc.utils.mail import VscMailError


class TestAuxiliary(TestCase):
    """
    Stuff that does not belong anywhere else :)
    """

    @mock.patch('vsc.filesystem.quota.tools.pwd.getpwall')
    def test_map_uids_to_names(self, mock_getpwall):
        """
        Check that the remapping functions properly
        """
        uids = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]

        mock_getpwall.return_value = uids
        res = tools.map_uids_to_names()

        self.assertEqual(res, {3: 1, 6: 4, 9: 7})


class TestNotifications(TestCase):
    """
    Tests for notyfin users and VO admins when exceeding quota
    """
    @mock.patch('vsc.filesystem.quota.tools.notify_exceeding_items')
    def test_notify_exceeding_users(self, mock_notify):
        """
        test the correctness of argument passing
        """

        kwargs = {
            'arg1': 'arg1',
            'arg2': 'arg2',
        }
        tools.notify_exceeding_users(**kwargs)
        kwargs['target'] = 'users'
        mock_notify.assert_called_with(**kwargs)

    @mock.patch('vsc.filesystem.quota.tools.notify_exceeding_items')
    def test_notify_exceeding_filesets(self, mock_notify):
        """
        test the correctness of argument passing
        """

        kwargs = {
            'arg1': 'arg1',
            'arg2': 'arg2',
        }
        tools.notify_exceeding_filesets(**kwargs)
        kwargs['target'] = 'filesets'
        mock_notify.assert_called_with(**kwargs)

    @mock.patch('vsc.filesystem.quota.tools.FileCache', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.notify')
    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_notify_exceeding_items_without_cache_updates(self, mock_client, mock_notify, mock_filecache):

        filesystem = 'vulpixhome'
        gpfs = mock.MagicMock()
        gpfs.list_filesystems.return_value = {filesystem: {'defaultMountPoint': '/here/we/are'}}

        mock_filecache.return_value = mock.MagicMock()
        mock_filecache_instance = mock_filecache.return_value
        mock_filecache_instance.update.return_value = False

        storage = VSC_HOME
        exceeding_items = [('item1', 123)]
        target = 'users'

        tools.notify_exceeding_items(gpfs, storage, filesystem, exceeding_items, target, mock_client)

        mock_filecache_instance.update.assert_called_with('item1', 123, tools.QUOTA_NOTIFICATION_CACHE_THRESHOLD)
        mock_filecache_instance.close.assert_not_called()

    @mock.patch('vsc.filesystem.quota.tools.FileCache', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.notify')
    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_notify_exceeding_items_with_cache_updates(self, mock_client, mock_notify, mock_filecache):

        filesystem = 'vulpixhome'
        gpfs = mock.MagicMock()
        gpfs.list_filesystems.return_value = {filesystem: {'defaultMountPoint': '/here/we/are'}}

        mock_filecache.return_value = mock.MagicMock()
        mock_filecache_instance = mock_filecache.return_value
        mock_filecache_instance.update.return_value = True

        storage = VSC_HOME
        exceeding_items = [('item1', 123)]
        target = 'users'

        tools.notify_exceeding_items(gpfs, storage, filesystem, exceeding_items, target, mock_client)

        mock_filecache_instance.update.assert_called_with('item1', 123, tools.QUOTA_NOTIFICATION_CACHE_THRESHOLD)
        mock_filecache_instance.close.assert_called_with()
        mock_notify.assert_called_with(storage, 'item1', 123, mock_client, False)

    @mock.patch('vsc.filesystem.quota.tools.VscMail', autospec=True)
    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.VscTier2AccountpageUser', autospec=True)
    def test_notify_unknown_email_user(self, mock_user, mock_client, mock_mail):

        storage_name = VSC_DATA
        item = 'vsc40075'
        quota = QuotaUser(storage_name, 'vulpixdata', item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        mock_mail.return_value = mock.MagicMock()
        mock_mail_instance = mock_mail.return_value
        mock_mail_instance.sendTextMail.return_value = None
        mock_mail_instance.sendTextMail.side_effect = VscMailError("Unknown email address")

        mock_user.return_value = mock.MagicMock()
        mock_user_instance = mock_user.return_value
        mock_user_instance.account = mkVscAccount({
            'vsc_id': 'vsc40075',
            'status': 'active',
            'vsc_id_number': '2540075',
            'home_directory': '/does/not/exist/home',
            'data_directory': '/does/not/exist/data',
            'scratch_directory': '/does/not/exist/scratch',
            'login_shell': '/bin/bash',
            'broken': False,
            'email': 'vsc40075@example.com',
            'research_field': 'many',
            'create_timestamp': '200901010000Z',
            'person': {
                'gecos': 'Willy Wonka',
                'institute': 'gent',
                'institute_login': 'wwonka',
            },
        })
        mock_user_instance.person = mkVscAccountPerson({
            'gecos': 'Willy Wonka',
            'institute': 'gent',
            'institute_login': 'wwonka',
        })

        message = tools.QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(
            user_name=mock_user_instance.person.gecos,
            vo_name=item,
            storage_name=storage_name,
            quota_info="%s" % (quota,),
            time=time.ctime()
        )

        tools.notify(storage_name, item, quota, mock_client)
        mock_mail_instance.sendTextMail.has_calls()


    @mock.patch('vsc.filesystem.quota.tools.VscMail', autospec=True)
    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.VscTier2AccountpageUser', autospec=True)
    def test_notify_user(self, mock_user, mock_client, mock_mail):

        storage_name = VSC_DATA
        item = 'vsc40075'
        quota = QuotaUser(storage_name, 'vulpixdata', item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        mock_mail.return_value = mock.MagicMock()
        mock_mail_instance = mock_mail.return_value
        mock_mail_instance.sendTextMail.return_value = None

        mock_user.return_value = mock.MagicMock()
        mock_user_instance = mock_user.return_value
        mock_user_instance.account = mkVscAccount({
            'vsc_id': 'vsc40075',
            'status': 'active',
            'vsc_id_number': '2540075',
            'home_directory': '/does/not/exist/home',
            'data_directory': '/does/not/exist/data',
            'scratch_directory': '/does/not/exist/scratch',
            'login_shell': '/bin/bash',
            'broken': False,
            'email': 'vsc40075@example.com',
            'research_field': 'many',
            'create_timestamp': '200901010000Z',
            'person': {
                'gecos': 'Willy Wonka',
                'institute': 'gent',
                'institute_login': 'wwonka',
            },
        })
        mock_user_instance.person = mkVscAccountPerson({
            'gecos': 'Willy Wonka',
            'institute': 'gent',
            'institute_login': 'wwonka',
        })

        message = tools.QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(
            user_name=mock_user_instance.person.gecos,
            vo_name=item,
            storage_name=storage_name,
            quota_info="%s" % (quota,),
            time=time.ctime()
        )

        tools.notify(storage_name, item, quota, mock_client)
        mock_mail_instance.sendTextMail.has_calls()

    @mock.patch('vsc.filesystem.quota.tools.VscMail', autospec=True)
    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.VscTier2AccountpageUser', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.VscTier2AccountpageVo', autospec=True)
    def test_notify_vo(self, mock_vo, mock_user, mock_client, mock_mail):

        storage_name = VSC_DATA
        item = 'gvo00002'
        quota = QuotaUser(storage_name, 'vulpixdata', item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        mock_mail.return_value = mock.MagicMock()
        mock_mail_instance = mock_mail.return_value
        mock_mail_instance.sendTextMail.return_value = None

        mock_user.return_value = mock.MagicMock()
        mock_user_instance = mock_user.return_value
        mock_user_instance.account = mkVscAccount({
            'vsc_id': 'vsc40075',
            'status': 'active',
            'vsc_id_number': 2540075,
            'home_directory': '/does/not/exist/home',
            'data_directory': '/does/not/exist/data',
            'scratch_directory': '/does/not/exist/scratch',
            'login_shell': '/bin/bash',
            'broken': False,
            'email': 'vsc40075@example.com',
            'research_field': 'many',
            'create_timestamp': '200901010000Z',
            'person': {
                'gecos': 'Willy Wonka',
                'institute': 'gent',
                'institute_login': 'wwonka',
            },
        })
        mock_user_instance.person = mkVscAccountPerson({
            'gecos': 'Willy Wonka',
            'institute': 'gent',
            'institute_login': 'wwonka',
        })


        mock_vo.return_value = mock.MagicMock()
        mock_vo_instance = mock_vo.return_value
        mock_vo_instance.vo = mkVo({
            'vsc_id': 'gvo00002',
            'status': 'active',
            'vsc_id_number': 2640002,
            'institute': 'gent',
            'fairshare': 0,
            'data_path': '/does/not/exist/data',
            'scratch_path': 'does/not/exist/scratch',
            'description': 'sputum',
            'members': ['vsc40075'],
            'moderators': ['vsc40075'],
        })

        tools.notify(storage_name, item, quota, mock_client)

        message = tools.VO_QUOTA_EXCEEDED_MAIL_TEXT_TEMPLATE.safe_substitute(
            user_name=mock_user_instance.person.gecos,
            vo_name=item,
            storage_name=storage_name,
            quota_info="%s" % (quota,),
            time=time.ctime()
        )

        mock_mail_instance.sendTextMail.assert_called_once_with(
            mail_to=mock_user_instance.account.email,
            mail_from="hpc@ugent.be",
            reply_to="hpc@ugent.be",
            mail_subject="Quota on %s exceeded" % (storage_name,),
            message=message,
        )


class TestProcessing(TestCase):

    @mock.patch('vsc.filesystem.quota.tools.os')
    @mock.patch('vsc.filesystem.quota.tools.push_user_quota_to_django')
    @mock.patch('vsc.filesystem.quota.tools.VscTier2AccountpageUser', autospec=True)
    def test_process_user_quota_no_store(self, mock_user, mock_push_quota, mock_os):

        storage_name = VSC_DATA
        item = 'vsc40075'
        filesystem = 'vulpixdata'
        quota = QuotaUser(storage_name, filesystem, item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = mock.MagicMock()
        storage[storage_name] = mock.MagicMock()
        storage[storage_name].login_mount_point = "/my_login_mount_point"
        storage[storage_name].gpfs_mount_point = "/my_gpfs_mount_point"
        storage.path_templates = mock.MagicMock()

        gpfs = mock.MagicMock()
        gpfs.is_symlink.return_value = False

        client = mock.MagicMock()

        quota_map = {'2540075': quota}
        user_map = {2540075: 'vsc40075'}

        store_cache = False

        tools.process_user_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, store_cache, dry_run=False)

        mock_os.stat.assert_not_called()
        mock_push_quota.assert_called_with(user_map, storage_name, storage.path_templates[storage_name], quota_map, client, False)

    @mock.patch('vsc.filesystem.quota.tools.FileCache', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.os')
    @mock.patch('vsc.filesystem.quota.tools.push_user_quota_to_django')
    @mock.patch('vsc.filesystem.quota.tools.VscTier2AccountpageUser', autospec=True)
    def test_process_user_quota_store(self, mock_user, mock_push_quota, mock_os, mock_cache):

        storage_name = VSC_DATA
        item = 'vsc40075'
        filesystem = 'vulpixdata'
        quota = QuotaUser(storage_name, filesystem, item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        mock_user.return_value = mock.MagicMock()
        mock_user_instance = mock_user.return_value
        mock_user_instance._get_path.return_value = "/my_path"

        storage = mock.MagicMock()
        storage[storage_name] = mock.MagicMock()
        storage[storage_name].login_mount_point = "/my_login_mount_point"
        storage[storage_name].gpfs_mount_point = "/my_gpfs_mount_point"
        storage.path_templates = mock.MagicMock()
        storage.path_templates[storage_name] = mock.MagicMock()

        gpfs = mock.MagicMock()
        gpfs.is_symlink.return_value = False

        client = mock.MagicMock()

        quota_map = {'2540075': quota}
        user_map = {2540075: 'vsc40075'}

        store_cache = True

        tools.process_user_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, user_map, client, store_cache, dry_run=False)

        mock_os.stat.assert_called_with("/my_path")
        mock_push_quota.assert_not_called()

    @mock.patch('vsc.filesystem.quota.tools.os')
    @mock.patch('vsc.filesystem.quota.tools.push_vo_quota_to_django')
    def test_process_fileset_quota_no_store(self, mock_push_quota, mock_os):

        storage_name = VSC_DATA
        filesystem = 'vulpixdata'
        fileset = 'gvo00002'
        quota = QuotaFileset(storage_name, filesystem, fileset)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = mock.MagicMock()

        gpfs = mock.MagicMock()
        gpfs.list_filesets.return_value = {
            filesystem: {
                fileset: {
                    'path': '/my_path',
                    'filesetName': fileset,
                }
            }
        }

        client = mock.MagicMock()

        quota_map = {fileset: quota}

        store_cache = False

        tools.process_fileset_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, client, store_cache, dry_run=False)

        mock_os.stat.assert_not_called()
        mock_push_quota.assert_called_with(storage_name, quota_map, client, False)

    @mock.patch('vsc.filesystem.quota.tools.FileCache', autospec=True)
    @mock.patch('vsc.filesystem.quota.tools.os')
    @mock.patch('vsc.filesystem.quota.tools.push_vo_quota_to_django')
    def test_process_fileset_quota_store(self, mock_push_quota, mock_os, mock_cache):

        storage_name = VSC_DATA
        filesystem = 'vulpixdata'
        fileset = 'gvo00002'
        quota = QuotaUser(storage_name, filesystem, fileset)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = mock.MagicMock()

        gpfs = mock.MagicMock()
        gpfs.list_filesets.return_value = {
            filesystem: {
                fileset: {
                    'path': '/my_path',
                    'filesetName': fileset,
                }
            }
        }

        client = mock.MagicMock()

        quota_map = {fileset: quota}

        store_cache = True

        tools.process_fileset_quota_store_optional(storage, gpfs, storage_name, filesystem, quota_map, client, store_cache, dry_run=False)

        mock_os.stat.assert_called_with("/my_path")
        mock_push_quota.assert_not_called()
