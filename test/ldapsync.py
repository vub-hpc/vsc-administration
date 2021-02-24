#
# Copyright 2015-2021 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
Tests for vsc.administration.user

@author: Andy Georges (Ghent University)
@author: Jens Timmerman (Ghent University)
"""
import mock
import vsc

from unittest import skipIf

from vsc.install.testing import TestCase
from vsc.accountpage.wrappers import mkVscAccountPubkey, mkVscAccount, mkGroup

from vsc.administration.ldapsync import LdapSyncer, UPDATED
from vsc.ldap.entities import VscLdapUser, VscLdapGroup
from vsc.utils.py2vs3 import HTTPError, is_py2, is_py3

from .user import test_account_1, test_account_4, test_usergroup_1, test_pubkeys_1, test_unicode_pubkeys

test_quota = [
    {
        "user": "vsc10018",
        "storage": {
            "institute": "brussel",
            "name": "VSC_HOME",
            "storage_type": "home"
        },
        "fileset": "None",
        "hard": 5242880
    }
]

test_vo_1 = {
    "vsc_id": "gvo00003",
    "status": "active",
    "vsc_id_number": 2640010,
    "institute": {
        "name": "gent"
    },
    "fairshare": 100,
    "data_path": "/user/data/gent/gvo000/gvo00003",
    "scratch_path": "/user/scratch/gent/gvo000/gvo00003",
    "description": "VO",
    "members": [
        "vsc40075",
    ],
    "moderators": [
        "vsc40075"
    ]
}

test_group_1 = {
    "vsc_id": "gtestgroup",
    "status": "inactive",
    "vsc_id_number": 2640011,
    "institute": {
        "name": "gent"
    },
    "description": "a test group",
    "members": [
        "vsc40075",
    ],
    "moderators": [
    ]
}



class LDAPSyncerTest(TestCase):
    """
    Tests for the LDAP syncer that sync account page information to the vsc ldap.
    """

    @mock.patch.object(vsc.administration.ldapsync.LdapSyncer, 'add_or_update')
    def test_sync_altered_accounts(self, mock_add_or_update):
        """Test the sync_altered accounts function"""
        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_1)
        mock_client.account[test_account.vsc_id] = mock.MagicMock()
        mock_client.account.modified[1].get.return_value = (200, [test_account_1])
        mock_client.account[test_account.vsc_id].usergroup.get.return_value = (200, test_usergroup_1)
        mock_client.get_public_keys.return_value = [mkVscAccountPubkey(p) for p in test_pubkeys_1]
        mock_client.account[test_account.vsc_id].quota.get.return_value = (200, test_quota)

        mock_add_or_update.return_value = UPDATED
        ldapsyncer = LdapSyncer(mock_client)
        accounts = ldapsyncer.sync_altered_accounts(1)
        self.assertEqual(accounts, {'error': set([]), 'new': set([]), 'updated': set([test_account.vsc_id])})
        ldap_attrs = {'status': ['active'], 'scratchDirectory': ['/user/scratch/gent/vsc400/vsc40075'], 'dataDirectory': ['/user/data/gent/vsc400/vsc40075'], 'cn': 'vsc40075', 'homeQuota': ['5242880'], 'institute': ['gent'], 'loginShell': ['/bin/bash'], 'uidNumber': ['2540075'], 'researchField': ['Bollocks'], 'gidNumber': ['2540075'], 'gecos': ['Foo Bar'], 'dataQuota': ['1'], 'homeDirectory': ['/user/home/gent/vsc400/vsc40075'], 'mail': ['foobar@ugent.be'], 'scratchQuota': ['1'], 'pubkey': ['pubkey1', 'pubkey2'], 'instituteLogin': ['foobar'], 'uid': ['vsc40075']}
        mock_add_or_update.assert_called_with(VscLdapUser, test_account.vsc_id, ldap_attrs, True)

    @mock.patch.object(vsc.administration.ldapsync.LdapSyncer, 'add_or_update')
    def test_sync_altered_accounts_unicode_pubkey(self, mock_add_or_update):
        """Test the sync_altered accounts function with a pubkey containing unicode"""
        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_1)
        mock_client.account[test_account.vsc_id] = mock.MagicMock()
        mock_client.account.modified[1].get.return_value = (200, [test_account_1])
        mock_client.account[test_account.vsc_id].usergroup.get.return_value = (200, test_usergroup_1)
        mock_client.get_public_keys.return_value = [mkVscAccountPubkey(p) for p in test_unicode_pubkeys]
        mock_client.account[test_account.vsc_id].quota.get.return_value = (200, test_quota)

        mock_add_or_update.return_value = UPDATED
        ldapsyncer = LdapSyncer(mock_client)
        accounts = ldapsyncer.sync_altered_accounts(1)
        self.assertEqual(accounts, {'error': set([]), 'new': set([]), 'updated': set([test_account.vsc_id])})
        if is_py2():
            expected_key = 'some pubkey \\u201chuppelde@daar.com\\u201d'
        else:
            expected_key = 'some pubkey \\xe2\\x80\\x9chuppelde@daar.com\\xe2\\x80\\x9d'
        ldap_attrs = {'status': ['active'], 'scratchDirectory': ['/user/scratch/gent/vsc400/vsc40075'], 'dataDirectory': ['/user/data/gent/vsc400/vsc40075'], 'cn': 'vsc40075', 'homeQuota': ['5242880'], 'institute': ['gent'], 'loginShell': ['/bin/bash'], 'uidNumber': ['2540075'], 'researchField': ['Bollocks'], 'gidNumber': ['2540075'], 'gecos': ['Foo Bar'], 'dataQuota': ['1'], 'homeDirectory': ['/user/home/gent/vsc400/vsc40075'], 'mail': ['foobar@ugent.be'], 'scratchQuota': ['1'], 'pubkey': [expected_key], 'instituteLogin': ['foobar'], 'uid': ['vsc40075']}
        mock_add_or_update.assert_called_with(VscLdapUser, test_account.vsc_id, ldap_attrs, True)

    @mock.patch.object(vsc.administration.ldapsync.LdapSyncer, 'add_or_update')
    def test_sync_forceinactive_account(self, mock_add_or_update):
        """Test the sync_altered accounts function"""
        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_4)
        mock_client.account[test_account.vsc_id] = mock.MagicMock()
        mock_client.account.modified[1].get.return_value = (200, [test_account_4])
        mock_client.account[test_account.vsc_id].usergroup.get.return_value = (200, test_usergroup_1)
        mock_client.get_public_keys.return_value = [mkVscAccountPubkey(p) for p in test_pubkeys_1]
        mock_client.account[test_account.vsc_id].quota.get.return_value = (200, test_quota)

        mock_add_or_update.return_value = UPDATED
        ldapsyncer = LdapSyncer(mock_client)
        accounts = ldapsyncer.sync_altered_accounts(1)
        self.assertEqual(accounts, {'error': set([]), 'new': set([]), 'updated': set([test_account.vsc_id])})
        ldap_attrs = {'status': ['inactive'], 'scratchDirectory': ['/scratch/brussel/vsc100/vsc10001'], 'dataDirectory': ['/data/brussel/vsc100/vsc10001'], 'cn': 'vsc10004', 'homeQuota': ['5242880'], 'institute': ['brussel'], 'loginShell': ['/bin/bash'], 'uidNumber': ['2510004'], 'researchField': ['Dinges'], 'gidNumber': ['2540075'], 'gecos': ['Foo Bar'], 'dataQuota': ['1'], 'homeDirectory': ['/user/brussel/vsc100/vsc10001'], 'mail': ['foobar@vub.ac.be'], 'scratchQuota': ['1'], 'pubkey': ['pubkey1', 'pubkey2'], 'instituteLogin': ['fooby'], 'uid': ['vsc10004']}
        mock_add_or_update.assert_called_with(VscLdapUser, test_account.vsc_id, ldap_attrs, True)


    @mock.patch.object(vsc.administration.ldapsync.LdapSyncer, 'add_or_update')
    def test_sync_altered_groups(self, mock_add_or_update):
        """Test the sync_altered accounts function"""
        mock_client = mock.MagicMock()
        test_group = mkGroup(test_vo_1)
        mock_client.allgroups.modified[1].get.return_value = (200, [test_vo_1])
        mock_client.vo[test_group.vsc_id].get.return_value = (200, test_vo_1)

        mock_add_or_update.return_value = UPDATED
        ldapsyncer = LdapSyncer(mock_client)
        groups = ldapsyncer.sync_altered_groups(1)
        self.assertEqual(groups, {'error': set([]), 'new': set([]), 'updated': set([test_group.vsc_id])})

        ldap_attrs = {'status': ['active'], 'scratchDirectory': ['/user/scratch/gent/gvo000/gvo00003'],
                      'dataDirectory': ['/user/data/gent/gvo000/gvo00003'], 'cn': 'gvo00003', 'institute': ['gent'],
                      'memberUid': ['vsc40075'], 'moderator': ['vsc40075'], 'gidNumber': ['2640010'],
                      'fairshare': ['100'], 'description': ['VO']}
        mock_add_or_update.assert_called_with(VscLdapGroup, test_group.vsc_id, ldap_attrs, True)

        #  raise a 404 error on getting the VO to trigger a non vo group add instead of a vo add as above
        test_group = mkGroup(test_usergroup_1)
        mock_client.allgroups.modified[1].get.return_value = (200, [test_usergroup_1])
        mock_client.vo[test_group.vsc_id].get.side_effect = HTTPError("mock_url", 404, "Not Found", "mock_headers", None)
        groups = ldapsyncer.sync_altered_groups(1)
        self.assertEqual(groups, {'error': set([]), 'new': set([]), 'updated': set([test_group.vsc_id])})
        ldap_attrs =  {'status': ['active'], 'cn': 'vsc40075', 'institute': ['gent'], 'memberUid': ['vsc40075'],
                       'moderator': ['vsc40075'], 'gidNumber': ['2540075']}
        mock_add_or_update.assert_called_with(VscLdapGroup, test_group.vsc_id, ldap_attrs, True)

    @mock.patch.object(vsc.administration.ldapsync.LdapSyncer, 'add_or_update')
    def test_sync_group_without_moderators(self, mock_add_or_update):
        """Test the synchronisation of the group without active mods."""
        mock_client = mock.MagicMock()
        test_group = mkGroup(test_group_1)
        mock_client.allgroups.modified[1].get.return_value = (200, [test_group_1])
        mock_client.vo[test_group.vsc_id].get.side_effect = HTTPError(
            None,
            404,
            'not found',
            None,
            None
        )

        mock_add_or_update.return_value = UPDATED

        ldapsyncer = LdapSyncer(mock_client)
        groups = ldapsyncer.sync_altered_groups(1)
        self.assertEqual(groups, {'error': set([]), 'new': set([]), 'updated': set([test_group.vsc_id])})

        ldap_attrs = {
            'status': ['inactive'],
            'cn': 'gtestgroup',
            'institute': ['gent'],
            'memberUid': ['vsc40075'],
            'moderator': ['vsc40003'],
            'gidNumber': ['2640011'],
        }
        mock_add_or_update.assert_called_with(VscLdapGroup, test_group.vsc_id, ldap_attrs, True)

