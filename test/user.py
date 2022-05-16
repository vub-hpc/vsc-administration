#
# Copyright 2015-2022 Ghent University
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
import os

from collections import namedtuple

import mock

import vsc.administration.user as user
import vsc.config.base as config

from vsc.accountpage.wrappers import mkVscAccount, mkVscHomeOnScratch, mkUserGroup, mkGroup
from vsc.accountpage.wrappers import mkVscAccountPubkey
from vsc.config.base import VSC_DATA, VSC_HOME, GENT, BRUSSEL, VSC_PRODUCTION_SCRATCH
from vsc.install.testing import TestCase

# monkey patch location of storage configuration file to included test config
config.STORAGE_CONFIGURATION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filesystem_info.conf')

test_account_1 = {
    u'broken': False,
    u'create_timestamp': u'1970-01-01T00:00:00.197Z',
    u'data_directory': u'/user/data/gent/vsc400/vsc40075',
    u'email': u'foobar@ugent.be',
    u'home_directory': u'/user/home/gent/vsc400/vsc40075',
    u'login_shell': u'/bin/bash',
    u'person': {
        u'gecos': u'Foo Bar',
        u'institute': {u'name': u'gent'},
        u'institute_login': u'foobar',
        u'institute_affiliation': u'staff',
        u'realeppn': u'foobar@ugent.be',
    },
    u'research_field': [u'Bollocks', u'Pluto'],
    u'scratch_directory': u'/user/scratch/gent/vsc400/vsc40075',
    u'status': u'active',
    u'vsc_id': u'vsc40075',
    u'vsc_id_number': 2540075,
    u'expiry_date': u'2032-01-01',
    u'home_on_scratch': False,
    u'force_active': False,
    u"isactive": True,
}

test_account_2 = {
    u'broken': False,
    u'create_timestamp': u'1970-01-01T00:00:00.197Z',
    u'data_directory': u'/user/data/gent/vsc400/vsc40075',
    u'email': u'foobar@ugent.be',
    u'home_directory': u'/user/home/gent/vsc400/vsc40075',
    u'login_shell': u'/bin/bash',
    u'person': {
        u'gecos': u'Foo Bar',
        u'institute': {u'name': u'gent'},
        u'institute_login': u'x_admin',
        u'institute_affiliation': u'staff',
        u'realeppn': u'x_admin@ugent.be',
    },
    u'research_field': [u'Bollocks', u'Pluto'],
    u'scratch_directory': u'/user/scratch/gent/vsc400/vsc40075',
    u'status': u'active',
    u'vsc_id': u'vsc40075',
    u'vsc_id_number': 2540075,
    u'expiry_date': u'2034-01-01',
    u'home_on_scratch': False,
    u'force_active': False,
    u"isactive": True,
}

test_account_3 = {
    u'broken': False,
    u'create_timestamp': u'1970-01-01T00:00:00.297Z',
    u'data_directory': u'/data/brussel/vsc100/vsc10001',
    u'email': u'foobar@vub.ac.be',
    u'home_directory': u'/user/brussel/vsc100/vsc10001',
    u'login_shell': u'/bin/bash',
    u'person': {
        u'gecos': u'Foo Bar',
        u'institute': {u'name': u'brussel'},
        u'institute_login': u'fooby',
        u'institute_affiliation': u'staff',
        u'realeppn': u'fooby@vub.ac.be',
    },
    u'research_field': [u'Dinges', u'Pluto'],
    u'scratch_directory': u'/scratch/brussel/vsc100/vsc10001',
    u'status': u'active',
    u'vsc_id': u'vsc10001',
    u'vsc_id_number': 2510001,
    u'expiry_date': u'2030-01-01',
    u'home_on_scratch': False,
    u'force_active': False,
    u"isactive": True,
}

test_account_4 = {
    u'broken': False,
    u'create_timestamp': u'1970-01-01T00:00:00.297Z',
    u'data_directory': u'/data/brussel/vsc100/vsc10001',
    u'email': u'foobar@vub.ac.be',
    u'home_directory': u'/user/brussel/vsc100/vsc10001',
    u'login_shell': u'/bin/bash',
    u'person': {
        u'gecos': u'Foo Bar',
        u'institute': {u'name': u'brussel'},
        u'institute_login': u'fooby',
        u'institute_affiliation': u'staff',
        u'realeppn': u'fooby@vub.ac.be',
    },
    u'research_field': [u'Dinges', u'Pluto'],
    u'scratch_directory': u'/scratch/brussel/vsc100/vsc10001',
    u'status': u'forceinactive',
    u'vsc_id': u'vsc10004',
    u'vsc_id_number': 2510004,
    u'expiry_date': u'2030-01-01',
    u'home_on_scratch': False,
    u'force_active': False,
    u"isactive": False,
}

test_usergroup_1 = {
    "vsc_id": "vsc40075",
    "vsc_id_number": 2540075,
    "status": "active",
    "institute": {
        "name": "gent"
    },
    "members": [
        "vsc40075"
    ],
    "moderators": [
        "vsc40075"
    ],
    "description": "Nope"
}

test_admin_group_1 = {
    "vsc_id": "vsc40003",
    "status": "active",
    "vsc_id_number": 2540003,
    "institute": {
        "name": "gent"
    },
    "members": [],
    "moderators": [],
    "description": ""
}

test_pubkeys_1 = [
    {
        "pubkey": "pubkey1",
        "deleted": False,
        "vsc_id": "vsc40075"
    },
    {
        "pubkey": "pubkey2",
        "deleted": False,
        "vsc_id": "vsc40075"
    }
]

test_unicode_pubkeys = [
    {
        "pubkey": u"some pubkey \u201chuppelde@daar.com\u201d",
        "deleted": False,
        "vsc_id": "vsc40075"
    }
]

test_hos_1 = [
    {
        "account": {
            "vsc_id": "vsc40075",
            "status": "active",
            "isactive": True,
            "expiry_date": None,
            "grace_until": None,
            "vsc_id_number": 2540075,
            "home_directory": "/user/home/gent/vsc400/vsc40075",
            "data_directory": "/user/data/gent/vsc400/vsc40075",
            "scratch_directory": "/user/scratch/gent/vsc400/vsc40075",
            "login_shell": "/bin/bash",
            "broken": False,
            "email": "andy.georges@ugent.be",
            "research_field": [
                "Computer systems, architectures, networks",
                "nwo"
            ],
            "create_timestamp": "2014-04-23T09:11:22.460855Z",
            "person": {
                "gecos": "Andy Georges",
                "institute": {
                    "name": "gent"
                },
                "institute_login": "ageorges",
                "institute_affiliation": "staff",
                u'realeppn': u'ageorges@ugent.be',
            },
            u'home_on_scratch': False,
            u'force_active': False,
        },
        "storage": {
            "institute": "gent",
            "name": "VSC_SCRATCH_MUK",
            "storage_type": "scratch"
        }
    }
]

test_quota_1 = [
    {u'fileset': u'gvo00002',
     u'hard': 5111808000,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_SCRATCH_DELCATTY',
         u'storage_type': u'scratch'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'gvo00002',
     u'hard': 2044723200,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_SCRATCH_PHANPY',
         u'storage_type': u'scratch'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'gvo00002',
     u'hard': 1835008000,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_DATA',
         u'storage_type': u'data'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'project_gpilot',
     u'hard': 10737418240,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_SCRATCH_MUK',
         u'storage_type': u'scratch'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'vsc40075',
     u'hard': 308224000,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_SCRATCH_MUK',
         u'storage_type': u'scratch'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'vsc400',
     u'hard': 26214400,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_DATA',
         u'storage_type': u'data'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'vsc400',
     u'hard': 3145728,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_HOME',
         u'storage_type': u'home'
     },
     u'user': u'vsc40075'},
    {u'fileset': u'vsc400',
     u'hard': 26214400,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_SCRATCH_DELCATTY',
         u'storage_type': u'scratch'},
     u'user': u'vsc40075'},
    {u'fileset': u'vsc400',
     u'hard': 1024,
     u'storage': {
         u'institute': u'gent',
         u'name': u'VSC_SCRATCH_PHANPY',
         u'storage_type': u'scratch'},
     u'user': u'vsc40075'}
]

test_quota_2 = [
    {
        u'fileset': u'vsc100',
        u'hard': 12582912,
        u'storage': {
            u'institute': u'brussel',
            u'name': u'VSC_HOME',
            u'storage_type': u'home'
        },
        u'user': u'vsc10001'
    },
    {
        u'fileset': u'vsc100',
        u'hard': 104857600,
        u'storage': {
            u'institute': u'brussel',
            u'name': u'VSC_DATA',
            u'storage_type': u'data'
        },
        u'user': u'vsc10001'
    },
    {
        u'fileset': u'vsc100',
        u'hard': 104857600,
        u'storage': {
            u'institute': u'brussel',
            u'name': u'VSC_SCRATCH_THEIA',
            u'storage_type': u'scratch'
        },
        u'user': u'vsc10001'
    },
]

# no quota on home nor data, only scratch
test_quota_3 = [
    {
        u'fileset': u'vsc100',
        u'hard': 104857600,
        u'storage': {
            u'institute': u'brussel',
            u'name': u'VSC_SCRATCH_THEIA',
            u'storage_type': u'scratch'
        },
        u'user': u'vsc10001'
    },
]


class VscAccountPageUserTest(TestCase):
    """
    Tests for the base class of users derived from account page information.
    """

    def test_get_institute_prefix(self):

        tests = [(test_account_1, 'g'), (test_account_3, 'b')]

        for account, prefix in tests:
            test_account = mkVscAccount(account)
            mock_client = mock.MagicMock()
            accountpageuser = user.VscAccountPageUser(test_account.vsc_id, rest_client=mock_client,
                                                      account=test_account)

            self.assertEqual(accountpageuser.get_institute_prefix(), prefix)

    def test_account_instantiation(self):

        test_accounts = [test_account_1, test_account_3]

        for account in test_accounts:
            mock_client = mock.MagicMock()
            test_account = mkVscAccount(account)
            accountpageuser = user.VscAccountPageUser(test_account.vsc_id, rest_client=mock_client,
                                                      account=test_account)

            self.assertEqual(accountpageuser.account, test_account)

            mock_client.account[test_account.vsc_id].get.return_value = (200, account)
            accountpageuser = user.VscAccountPageUser(test_account.vsc_id, mock_client)

            self.assertEqual(accountpageuser.account, test_account)

    def test_person_instantiation(self):

        test_accounts = [test_account_1, test_account_3]

        for account in test_accounts:
            mock_client = mock.MagicMock()
            test_account = mkVscAccount(account)
            accountpageuser = user.VscAccountPageUser(
                test_account.vsc_id,
                rest_client=mock_client,
                account=test_account)

            self.assertEqual(accountpageuser.person, test_account.person)

            mock_client.account[test_account.vsc_id].get.return_value = (200, account)
            accountpageuser = user.VscAccountPageUser(test_account.vsc_id, mock_client)

            self.assertEqual(accountpageuser.person, test_account.person)

    def test_usergroup_instantiation(self):

        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_1)
        mock_client.account[test_account.vsc_id] = mock.MagicMock()
        mock_client.account[test_account.vsc_id].get.return_value = (200, test_account_1)
        mock_client.account[test_account.vsc_id].usergroup.get.return_value = (200, test_usergroup_1)

        accountpageuser = user.VscAccountPageUser(test_account.vsc_id, rest_client=mock_client)

        self.assertEqual(accountpageuser.usergroup, mkUserGroup(test_usergroup_1))

        mock_client.account[test_account.vsc_id].get.return_value = (200, test_account_2)
        mock_client.group[test_account_2].get.return_value = (200, test_admin_group_1)
        accountpageuser = user.VscAccountPageUser(test_account.vsc_id, mock_client)

        self.assertEqual(accountpageuser.usergroup, mkGroup(test_admin_group_1))

    def test_pubkeys_instantiation(self):

        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_1)
        mock_client.account[test_account.vsc_id].pubkey.get.return_value = (200, test_pubkeys_1)

        accountpageuser = user.VscAccountPageUser(test_account.vsc_id, rest_client=mock_client)

        self.assertEqual(set(accountpageuser.pubkeys), set([mkVscAccountPubkey(p) for p in test_pubkeys_1]))

    def test_homeonscratch_instantiation(self):

        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_1)
        mock_client.account[test_account.vsc_id].home_on_scratch.get.return_value = (200, test_hos_1)

        accountpageuser = user.VscAccountPageUser(test_account.vsc_id, rest_client=mock_client)

        self.assertEqual(accountpageuser.home_on_scratch, [mkVscHomeOnScratch(h) for h in test_hos_1])


class VscTier2AccountpageUserTest(TestCase):
    """
    Tests for the VscTier2AccountpageUser.
    """

    def test_init_quota(self):

        tests = [
            (test_account_1, test_quota_1, GENT, 'vsc400'),
            (test_account_3, test_quota_2, BRUSSEL, 'vsc100'),
        ]

        def set_up_accountpageuser(account, quota, site):
            mock_client = mock.MagicMock()
            test_account = mkVscAccount(account)
            mock_client.account[test_account.vsc_id].quota.get.return_value = (200, quota)

            return user.VscTier2AccountpageUser(
                test_account.vsc_id,
                storage=config.VscStorage(),
                rest_client=mock_client,
                account=test_account,
                host_institute=site)

        for account, quota, site, fileset in tests:
            accountpageuser = set_up_accountpageuser(account, quota, site)

            self.assertEqual(
                accountpageuser.user_home_quota,
                [q['hard'] for q in quota if q['storage']['name'] == 'VSC_HOME' and q['fileset'] == fileset][0]
            )
            self.assertEqual(
                accountpageuser.user_data_quota,
                [q['hard'] for q in quota if q['storage']['name'] == 'VSC_DATA' and q['fileset'] == fileset][0]
            )
            self.assertEqual(
                [(q.fileset, q.hard, q.storage['name']) for q in accountpageuser.user_scratch_quota],
                [(q['fileset'], q['hard'], q['storage']['name']) for q in quota
                    if q['storage']['storage_type'] == 'scratch' and q['fileset'] == fileset]
            )

        # check if no home or data quota are set
        accountpageuser = set_up_accountpageuser(test_account_3, test_quota_3, BRUSSEL)
        self.assertEqual(accountpageuser.user_home_quota, None)
        self.assertEqual(accountpageuser.user_data_quota, None)
        self.assertNotEqual(accountpageuser.user_scratch_quota, None)


class UserDeploymentTest(TestCase):
    """
    Tests for the User deployment code.
    """

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_process_regular_users(self, mock_client):

        test_accounts = [(['vsc40075', 'vsc40123', 'vsc40039'], GENT), (['vsc10001'], BRUSSEL)]
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mock_client.return_value = mock.MagicMock()

        for accounts, site in test_accounts:
            for storage_name in (VSC_HOME, VSC_DATA,) + VSC_PRODUCTION_SCRATCH[site]:
                with mock.patch('vsc.administration.user.VscTier2AccountpageUser', autospec=True) as mock_user:
                    with mock.patch('vsc.administration.user.update_user_status') as mock_update_user_status:

                        mock_user.return_value = mock.MagicMock()
                        mock_user_instance = mock_user.return_value

                        user.process_users(options, accounts, storage_name, mock_client, host_institute=site)

                        mock_user_instance.set_scratch_quota.assert_not_called()
                        mock_user_instance.set_home_quota.assert_not_called()
                        mock_user_instance.set_data_quota.assert_not_called()

                        if storage_name in (VSC_HOME,):
                            mock_user_instance.create_scratch_dir.assert_not_called()
                            mock_user_instance.create_data_dir.assert_not_called()

                            self.assertEqual(mock_user_instance.create_home_dir.called, True)
                            self.assertEqual(mock_user_instance.populate_home_dir.called, True)
                            self.assertEqual(mock_update_user_status.called, True)

                        if storage_name in (VSC_DATA,):
                            mock_user_instance.create_home_dir.assert_not_called()
                            mock_user_instance.populate_home_dir.assert_not_called()
                            mock_update_user_status.assert_not_called()

                            mock_user_instance.create_scratch_dir.assert_not_called()

                            self.assertEqual(mock_user_instance.create_data_dir.called, True)

                        if storage_name in VSC_PRODUCTION_SCRATCH[site]:
                            mock_user_instance.create_home_dir.assert_not_called()
                            mock_user_instance.populate_home_dir.assert_not_called()
                            mock_update_user_status.assert_not_called()
                            mock_user_instance.create_data_dir.assert_not_called()

                            self.assertEqual(mock_user_instance.create_scratch_dir.called, True)

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_create_home_dir_tier2_user(self, mock_client):

        test_accounts = [(test_account_1, GENT), (test_account_3, BRUSSEL)]

        with mock.patch.object(config.Storage, 'load_operator') as mock_load_operator:
            backend_operator = mock.MagicMock()
            backend_operator_err = mock.MagicMock()
            mock_load_operator.return_value = (backend_operator, backend_operator_err)

            for account, site in test_accounts:
                test_account = mkVscAccount(account)
                accountpageuser = user.VscTier2AccountpageUser(
                    test_account.vsc_id,
                    storage=config.VscStorage(),
                    rest_client=mock_client,
                    account=test_account,
                    host_institute=site)
                accountpageuser.create_home_dir()

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_create_data_dir_tier2_user(self, mock_client):

        test_accounts = [(test_account_1, GENT), (test_account_3, BRUSSEL)]

        with mock.patch.object(config.Storage, 'load_operator') as mock_load_operator:
            backend_operator = mock.MagicMock()
            backend_operator_err = mock.MagicMock()
            mock_load_operator.return_value = (backend_operator, backend_operator_err)

            for account, site in test_accounts:
                test_account = mkVscAccount(account)
                accountpageuser = user.VscTier2AccountpageUser(
                    test_account.vsc_id,
                    storage=config.VscStorage(),
                    rest_client=mock_client,
                    account=test_account,
                    host_institute=site)
                accountpageuser.create_data_dir()

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_create_scratch_dir_tier2_user(self, mock_client):

        test_accounts = [(test_account_1, GENT), (test_account_3, BRUSSEL)]

        with mock.patch.object(config.Storage, 'load_operator') as mock_load_operator:
            backend_operator = mock.MagicMock()
            backend_operator_err = mock.MagicMock()
            mock_load_operator.return_value = (backend_operator, backend_operator_err)

            for account, site in test_accounts:
                test_account = mkVscAccount(account)
                accountpageuser = user.VscTier2AccountpageUser(
                    test_account.vsc_id,
                    storage=config.VscStorage(),
                    rest_client=mock_client,
                    account=test_account,
                    host_institute=site)
                accountpageuser.create_scratch_dir(VSC_PRODUCTION_SCRATCH[site][0])

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_process_regular_users_quota(self, mock_client):

        TestQuota = namedtuple("TestQuota", ['user'])
        test_accounts = [(['vsc40075', 'vsc40123', 'vsc40039'], GENT), (['vsc10001'], BRUSSEL)]

        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mock_client.return_value = mock.MagicMock()

        for accounts, site in test_accounts:
            test_quota = [TestQuota(user=u) for u in accounts]

            for storage_name in (VSC_HOME, VSC_DATA,) + VSC_PRODUCTION_SCRATCH[site]:
                with mock.patch('vsc.administration.user.VscTier2AccountpageUser', autospec=True) as mock_user:

                    mock_user.return_value = mock.MagicMock()
                    mock_user_instance = mock_user.return_value

                    user.process_users_quota(options, test_quota, storage_name, mock_client, host_institute=site)

                    if storage_name in (VSC_HOME,):
                        self.assertEqual(mock_user_instance.set_home_quota.called, True)

                        mock_user_instance.set_data_quota.assert_not_called()
                        mock_user_instance.set_scratch_quota.assert_not_called()

                    if storage_name in (VSC_DATA,):
                        self.assertEqual(mock_user_instance.set_data_quota.called, True)

                        mock_user_instance.set_home_quota.assert_not_called()
                        mock_user_instance.set_scratch_quota.assert_not_called()

                    if storage_name in VSC_PRODUCTION_SCRATCH[site]:
                        self.assertEqual(mock_user_instance.set_scratch_quota.called, True)

                        mock_user_instance.set_home_quota.assert_not_called()
                        mock_user_instance.set_data_quota.assert_not_called()
