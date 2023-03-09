#
# Copyright 2015-2023 Ghent University
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
Tests for vsc.administration.vo

@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""
import os
import pwd

from collections import namedtuple

import mock
from mock import patch

import vsc.administration.vo as vo
import vsc.config.base as config

from vsc.accountpage.wrappers import VscAutogroup
from vsc.config.base import (
    VSC_DATA, VSC_HOME, GENT_PRODUCTION_SCRATCH, VSC_DATA_SHARED,
    VSC_PRODUCTION_SCRATCH, BRUSSEL, GENT
)
from vsc.install.testing import TestCase


# monkey patch location of storage configuration file to included test config
config.STORAGE_CONFIGURATION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filesystem_info.conf')


class VoDeploymentTest(TestCase):
    """
    Tests for the VO deployment code.
    """

    @patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.base.VscStorage', autospec=True)
    def test_process_regular_vos(self, mock_storage, mock_client):
        """Test to see if the VscTier2AccountpageVo class is used properly"""

        test_vo_id = "gvo00002"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00002'].member.modified[date].get.return_value = (
            200, [{
                u'broken': False,
                u'create_timestamp': u'2014-04-23T09:11:22.460Z',
                u'data_directory': u'/user/data/gent/vsc400/vsc40075',
                u'email': u'andy.georges@ugent.be',
                u'home_directory': u'/user/home/gent/vsc400/vsc40075',
                u'login_shell': u'/bin/bash',
                u'person': {
                    u'gecos': u'Andy Georges',
                    u'institute': {u'name': u'gent'},
                    u'institute_login': u'ageorges'
                },
                u'research_field': [u'Computer systems, architectures, networks', u'nwo'],
                u'scratch_directory': u'/user/scratch/gent/vsc400/vsc40075',
                u'status': u'active',
                u'vsc_id': u'vsc40075',
                u'vsc_id_number': 2540075,
        }])

        for storage_name in (VSC_HOME, VSC_DATA) + GENT_PRODUCTION_SCRATCH:
            with patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                with patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                    with patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                        with patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                            with patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                                with patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                                    with patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                        with patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                            with patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                                with patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:
                                                    mock_user.return_value = mock.MagicMock()
                                                    ok, errors = vo.process_vos(options, [test_vo_id], storage_name, mc, date)
                                                    self.assertEqual(errors, {})

                                                    if storage_name in (VSC_HOME, VSC_DATA):
                                                        mock_cr_s_fileset.assert_not_called()
                                                        mock_s_s_quota.assert_not_called()

                                                    if storage_name in (VSC_DATA,):
                                                        self.assertEqual(mock_cr_d_fileset.called, True)
                                                        self.assertEqual(mock_s_d_quota.called, True)
                                                        self.assertEqual(mock_update_vo_status.called, True)

                                                        self.assertEqual(mock_s_m_d_quota.called, True)
                                                        self.assertEqual(mock_cr_m_d_dir.called, True)

                                                    else:
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        if storage_name not in (VSC_HOME,):
                                                            self.assertEqual(mock_cr_s_fileset.called, True)
                                                            self.assertEqual(mock_s_s_quota.called, True)

                                                            self.assertEqual(mock_s_m_s_quota.called, True)
                                                            self.assertEqual(mock_cr_m_s_dir.called, True)

    @patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.base.VscStorage', autospec=True)
    def test_process_non_gent_institute_vos(self, mock_storage, mock_client):

        test_vo_id = "gvo00018"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00018'].member.modified[date].get.return_value = (
            200, [{
                u'broken': False,
                u'create_timestamp': u'2014-04-23T09:11:22.460Z',
                u'data_directory': u'/user/leuven/data/vsc400/vsc40075',
                u'email': u'andy.georges@kuleuven.be',
                u'home_directory': u'/user/leuven/home/vsc400/vsc40075',
                u'login_shell': u'/bin/bash',
                u'person': {
                    u'gecos': u'Andy Georges',
                    u'institute': {u'name': u'leuven'},
                    u'institute_login': u'ageorges'
                },
                u'research_field': [u'Computer systems, architectures, networks', u'nwo'],
                u'scratch_directory': u'/user/leuven/scratch/vsc400/vsc40075',
                u'status': u'active',
                u'vsc_id': u'vsc40075',
                u'vsc_id_number': 2540075,
        }])

        for storage_name in (VSC_HOME, VSC_DATA) + GENT_PRODUCTION_SCRATCH:
            with patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                with patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                    with patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                        with patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                            with patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                                with patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                                    with patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                        with patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                            with patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                                with patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:

                                                    mock_user.return_value = mock.MagicMock()
                                                    ok, errors = vo.process_vos(options, [test_vo_id], storage_name, mc, "99991231")
                                                    self.assertEqual(errors, {})

                                                    if storage_name in (VSC_HOME, VSC_DATA):
                                                        mock_cr_s_fileset.assert_not_called()
                                                        mock_s_s_quota.assert_not_called()
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        mock_s_m_d_quota.assert_not_called()
                                                        mock_cr_m_d_dir.assert_not_called()

                                                    else:
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        self.assertEqual(mock_cr_s_fileset.called, True)
                                                        self.assertEqual(mock_s_s_quota.called, True)

                                                        self.assertEqual(mock_s_m_s_quota.called, True)
                                                        self.assertEqual(mock_cr_m_s_dir.called, True)

    @patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.base.VscStorage', autospec=True)
    def test_process_gent_institute_vo(self, mock_storage, mock_client):

        test_vo_id = "gvo00012"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00012'].member.modified[date].get.return_value = (
            200, [{
                u'broken': False,
                u'create_timestamp': u'2014-04-23T09:11:22.460Z',
                u'data_directory': u'/user/leuven/data/vsc400/vsc40075',
                u'email': u'andy.georges@kuleuven.be',
                u'home_directory': u'/user/leuven/home/vsc400/vsc40075',
                u'login_shell': u'/bin/bash',
                u'person': {
                    u'gecos': u'Andy Georges',
                    u'institute': {u'name': u'leuven'},
                    u'institute_login': u'ageorges'
                },
                u'research_field': [u'Computer systems, architectures, networks', u'nwo'],
                u'scratch_directory': u'/user/leuven/scratch/vsc400/vsc40075',
                u'status': u'active',
                u'vsc_id': u'vsc40075',
                u'vsc_id_number': 2540075,
        }])

        for storage_name in (VSC_HOME, VSC_DATA) + GENT_PRODUCTION_SCRATCH:
            with patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                with patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                    with patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                        with patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                            with patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                                with patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                                    with patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                        with patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                            with patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                                with patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:

                                                    mock_user.return_value = mock.MagicMock()
                                                    ok, errors = vo.process_vos(options, [test_vo_id], storage_name, mc, "99991231")
                                                    self.assertEqual(errors, {})

                                                    mock_cr_s_fileset.assert_not_called()
                                                    mock_s_s_quota.assert_not_called()
                                                    mock_cr_d_fileset.assert_not_called()
                                                    mock_s_d_quota.assert_not_called()
                                                    mock_update_vo_status.assert_not_called()

                                                    mock_s_m_d_quota.assert_not_called()
                                                    mock_cr_m_d_dir.assert_not_called()

    @patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.base.VscStorage', autospec=True)
    def test_process_gent_institute_vo_data_share(self, mock_storage, mock_client):

        test_vo_id = "gvo03442"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        v = mock.MagicMock()
        mc.vo[test_vo_id].get.return_value = v

        for storage_name in (VSC_DATA_SHARED,):
            with patch("vsc.administration.vo.VscTier2AccountpageVo.data_sharing", new_callable=mock.PropertyMock) as mock_data_sharing:
              with patch("vsc.administration.vo.VscTier2AccountpageVo.sharing_group", new_callable=mock.PropertyMock) as mock_sharing_group:
                with patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                  with patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                    with patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                      with patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                        with patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                          with patch.object(vo.VscTier2AccountpageVo, 'create_data_shared_fileset') as mock_cr_d_shared_fileset:
                            with patch.object(vo.VscTier2AccountpageVo, 'set_data_shared_quota') as mock_s_d_shared_quota:
                              with patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                with patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                  with patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                    with patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:

                                        mock_data_sharing.return_value = True
                                        mock_sharing_group.return_value = VscAutogroup(
                                            vsc_id=test_vo_id.replace('gvo', 'gvos'),
                                            status='active',
                                            vsc_id_number=123456,
                                            institute='Gent',
                                            members=['vsc40075'],
                                            description="test autogroup"
                                        )
                                        ok, errors = vo.process_vos(options, [test_vo_id], storage_name, mc, "99991231")
                                        self.assertEqual(errors, {})

                                        mock_cr_s_fileset.assert_not_called()
                                        mock_s_s_quota.assert_not_called()
                                        mock_cr_d_fileset.assert_not_called()
                                        mock_s_d_quota.assert_not_called()
                                        mock_cr_d_shared_fileset.assert_called()
                                        mock_s_d_shared_quota.assert_called()
                                        mock_update_vo_status.assert_not_called()

                                        mock_s_m_d_quota.assert_not_called()
                                        mock_cr_m_d_dir.assert_not_called()

    def test_create_sharing_fileset(self):

        test_vo_id = "gvo03442"

        mc = mock.MagicMock()
        mc.vo = mock.MagicMock()
        v = mock.MagicMock()
        mc.vo[test_vo_id].get.return_value = v

        with patch('vsc.administration.vo.mkVscAccount') as mock_mkvscaccount:
            mock_mkvscaccount.side_effect = IndexError("Nope")

            with patch('vsc.administration.base.StorageOperator') as mock_storage_operator:
                mock_storage_operator.return_value = mock.MagicMock()

                s = config.VscStorage()
                for storage in s[GENT]:
                    s[GENT][storage].operator = mock_storage_operator

                test_vo = vo.VscTier2AccountpageVo(test_vo_id, storage=s, rest_client=mc)

                with patch("vsc.administration.vo.VscTier2AccountpageVo.data_sharing", new_callable=mock.PropertyMock) as mock_data_sharing:
                    with patch("vsc.administration.vo.VscTier2AccountpageVo.sharing_group", new_callable=mock.PropertyMock) as mock_sharing_group:

                        mock_data_sharing.return_value = True
                        mock_sharing_group.return_value = VscAutogroup(
                            vsc_id=test_vo_id.replace('gvo', 'gvos'),
                            status='active',
                            vsc_id_number=123456,
                            institute='Gent',
                            members=['vsc40075'],
                            description="test autogroup"
                        )

                        test_vo.create_data_shared_fileset()

    @patch("vsc.accountpage.client.AccountpageClient", autospec=True)
    def test_process_brussel_vo(self, mock_client):
        """Test to see deploying a Brussel VO works fine"""
        test_vo_id = "bvo00005"
        date = "203112310000"

        Options = namedtuple("Options", ["dry_run"])
        options = Options(dry_run=False)

        # first mock all the calls to the accountpage
        mc = mock_client.return_value
        mc.account = mock.MagicMock()
        mc.vo = mock.MagicMock()
        account_1 = {
            "vsc_id": "vsc10001",
            "status": "active",
            "isactive": True,
            "force_active": False,
            "expiry_date": None,
            "grace_until": None,
            "vsc_id_number": 2510001,
            "home_directory": "/user/brussel/100/vsc10001",
            "data_directory": "/data/brussel/100/vsc10001",
            "scratch_directory": "/scratch/brussel/100/vsc10001",
            "login_shell": "/bin/bash",
            "broken": False,
            "email": "ward.poelmans@vub.ac.be",
            "research_field": ["Physics", "nwo"],
            "create_timestamp": "2018-11-13T14:27:53.394000Z",
            "person": {
                "gecos": "Ward Poelmans",
                "institute": {"name": "brussel"},
                "institute_login": "wapoelma",
                "institute_affiliation": "staff",
                "realeppn": "wapoelma@vub.ac.be",
            },
            "home_on_scratch": False,
        }
        mc.account[account_1["vsc_id"]].get.return_value = (200, account_1)
        mc.vo[test_vo_id].member.modified[date].get.return_value = (200, [account_1])
        mc.vo[test_vo_id].get.return_value = (
            200,
            {
                "vsc_id": "bvo00005",
                "status": "active",
                "vsc_id_number": 2610010,
                "institute": {"name": "brussel"},
                "fairshare": 100,
                "data_path": "/data/brussel/vo/000/bvo00005",
                "scratch_path": "/scratch/brussel/vo/000/bvo00005",
                "description": "hpcvub",
                "members": ["vsc10001", "vsc10003"],
                "moderators": ["vsc10001"],
            },
        )
        mc.vo[test_vo_id].quota.get.return_value = (
            200,
            [
                {
                    "virtual_organisation": "bvo00005",
                    "storage": {"institute": "brussel", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "bvo00005",
                    "hard": 104857600,
                },
                {
                    "virtual_organisation": "bvo00005",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "bvo00005",
                    "hard": 104857600,
                },
            ],
        )
        mc.account[account_1["vsc_id"]].quota.get.return_value = (
            200,
            [
                {
                    "user": "vsc10001",
                    "storage": {"institute": "brussel", "name": "VSC_HOME", "storage_type": "home"},
                    "fileset": "vsc100",
                    "hard": 12582912,
                },
                {
                    "user": "vsc10001",
                    "storage": {"institute": "brussel", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "vsc100",
                    "hard": 26214400,
                },
                {
                    "user": "vsc10001",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "vsc100",
                    "hard": 26214400,
                },
                {
                    "user": "vsc10001",
                    "storage": {"institute": "gent", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "gvo00016",
                    "hard": 131072000,
                },
                {
                    "user": "vsc10001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_DELCATTY", "storage_type": "scratch"},
                    "fileset": "gvo00016",
                    "hard": 131072000,
                },
                {
                    "user": "vsc10001",
                    "storage": {"institute": "brussel", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "bvo00005",
                    "hard": 52428800,
                },
                {
                    "user": "vsc10001",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "bvo00005",
                    "hard": 52428800,
                },
            ],
        )

        with patch('vsc.administration.base.StorageOperator') as mock_storage_operator:
            operator = mock.MagicMock()
            mock_storage_operator.return_value = operator

            # This shouldn't do anything
            ok, errors = vo.process_vos(options, [test_vo_id], VSC_HOME, mc, date, host_institute=BRUSSEL)
            self.assertEqual(errors, {})
            self.assertEqual(ok, {})

            # VSC_DATA test
            ok, errors = vo.process_vos(options, [test_vo_id], VSC_DATA, mc, date, host_institute=BRUSSEL)
            self.assertEqual(errors, {})
            self.assertEqual(ok, {"bvo00005": ["vsc10001"]})
            operator().list_filesets.assert_called()
            operator().get_fileset_info.assert_called_with("theiadata", "bvo00005")
            operator().chmod.assert_called_with(
                504, "/vscmnt/brussel_pixiu_data/_data_brussel/brussel/vo/000/bvo00005"
            )
            operator().chown.assert_called_with(
                2510001, 2610010, "/vscmnt/brussel_pixiu_data/_data_brussel/brussel/vo/000/bvo00005"
            )
            operator().set_fileset_quota.assert_called_with(
                102005473280, "/vscmnt/brussel_pixiu_data/_data_brussel/brussel/vo/000/bvo00005", "bvo00005", 107374182400
            )
            operator().set_fileset_grace.assert_called_with(
                "/vscmnt/brussel_pixiu_data/_data_brussel/brussel/vo/000/bvo00005", 604800
            )
            operator().set_user_quota.assert_called_with(
                soft=51002736640, user=2510001, obj="/vscmnt/brussel_pixiu_data/_data_brussel/brussel/vo/000/bvo00005", hard=53687091200
            )
            operator().create_stat_directory.assert_called_with(
                "/vscmnt/brussel_pixiu_data/_data_brussel/brussel/vo/000/bvo00005/vsc10001", 448, 2510001, 1, override_permissions=False
            )

            # VSC_SCRATCH test
            ok, errors = vo.process_vos(
                options, [test_vo_id], VSC_PRODUCTION_SCRATCH[BRUSSEL][0], mc, date, host_institute=BRUSSEL
            )
            self.assertEqual(errors, {})
            self.assertEqual(ok, {"bvo00005": ["vsc10001"]})
            operator().list_filesets.assert_called_with()
            operator().get_fileset_info.assert_called_with("theiascratch", "bvo00005")
            operator().chmod.assert_called_with(504, "/theia/scratch/brussel/vo/000/bvo00005")
            operator().chown.assert_called_with(2510001, 2610010, "/theia/scratch/brussel/vo/000/bvo00005")
            operator().set_fileset_quota.assert_called_with(
                102005473280, "/theia/scratch/brussel/vo/000/bvo00005", "bvo00005", 107374182400
            )
            operator().set_fileset_grace.assert_called_with("/theia/scratch/brussel/vo/000/bvo00005", 604800)
            operator().set_user_quota.assert_called_with(
                hard=53687091200, obj="/theia/scratch/brussel/vo/000/bvo00005", soft=51002736640, user=2510001
            )
            operator().create_stat_directory.assert_called_with(
                "/theia/scratch/brussel/vo/000/bvo00005/vsc10001", 448, 2510001, 1, override_permissions=False
            )

    @patch("vsc.accountpage.client.AccountpageClient", autospec=True)
    def test_process_brussel_default_vo(self, mock_client):
        """Test for a vsc1 account in the default Brussel VO"""
        test_vo_id = "bvo00001"
        date = "203012310000"

        Options = namedtuple("Options", ["dry_run"])
        options = Options(dry_run=False)

        # first mock all the calls to the accountpage
        mc = mock_client.return_value
        mc.account = mock.MagicMock()
        mc.vo = mock.MagicMock()
        account_1 = {
            "vsc_id": "vsc10002",
            "status": "active",
            "isactive": True,
            "force_active": False,
            "expiry_date": None,
            "grace_until": None,
            "vsc_id_number": 2510002,
            "home_directory": "/user/brussel/100/vsc10002",
            "data_directory": "/data/brussel/100/vsc10002",
            "scratch_directory": "/scratch/brussel/100/vsc10002",
            "login_shell": "/bin/bash",
            "broken": False,
            "email": "samuel.moors@vub.ac.be",
            "research_field": ["Chemistry", "nwo"],
            "create_timestamp": "2020-02-24T13:39:27.219855Z",
            "person": {
                "gecos": "Samuel Moors",
                "institute": {"name": "brussel"},
                "institute_login": "smoors",
                "institute_affiliation": "staff",
                "realeppn": "smoors@vub.ac.be",
            },
            "home_on_scratch": False,
        }
        mc.account[account_1["vsc_id"]].get.return_value = (200, account_1)
        mc.vo[test_vo_id].member.modified[date].get.return_value = (200, [account_1])
        mc.vo[test_vo_id].get.return_value = (
            200,
            {
                "vsc_id": "bvo00001",
                "status": "active",
                "vsc_id_number": 2610006,
                "institute": {"name": "brussel"},
                "fairshare": 100,
                "data_path": "/data/brussel/vo/000/bvo00001",
                "scratch_path": "/scratch/brussel/vo/000/bvo00001",
                "description": "default brussel VO at brussel",
                "members": ["vsc10002", "vsc10004"],
                "moderators": [],
            },
        )
        mc.vo[test_vo_id].quota.get.return_value = (
            200,
            [
                {
                    "virtual_organisation": "bvo00001",
                    "storage": {"institute": "brussel", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "bvo00001",
                    "hard": 104857600,
                },
                {
                    "virtual_organisation": "bvo00001",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "bvo00001",
                    "hard": 104857600,
                },
            ],
        )
        mc.account[account_1["vsc_id"]].quota.get.return_value = (
            200,
            [
                {
                    "user": "vsc10002",
                    "storage": {"institute": "gent", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "gvo00016",
                    "hard": 131072000,
                },
                {
                    "user": "vsc10002",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_DELCATTY", "storage_type": "scratch"},
                    "fileset": "gvo00016",
                    "hard": 131072000,
                },
                {
                    "user": "vsc10002",
                    "storage": {"institute": "brussel", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "bvo00001",
                    "hard": 52428800,
                },
                {
                    "user": "vsc10002",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "bvo00001",
                    "hard": 52428800,
                },
                {
                    "user": "vsc10002",
                    "storage": {"institute": "brussel", "name": "VSC_HOME", "storage_type": "home"},
                    "fileset": "vsc100",
                    "hard": 12582912,
                },
                {
                    "user": "vsc10002",
                    "storage": {"institute": "brussel", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "vsc100",
                    "hard": 104857600,
                },
                {
                    "user": "vsc10002",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "vsc100",
                    "hard": 104857600,
                },
            ],
        )

        with patch('vsc.administration.base.StorageOperator') as mock_storage_operator:
            mock_storage_operator.return_value = mock.MagicMock()

            # This shouldn't do anything
            ok, errors = vo.process_vos(options, [test_vo_id], VSC_HOME, mc, date, host_institute=BRUSSEL)
            self.assertEqual(errors, {})
            self.assertEqual(ok, {})

            # VSC_DATA should also not do anything
            ok, errors = vo.process_vos(options, [test_vo_id], VSC_DATA, mc, date, host_institute=BRUSSEL)
            self.assertEqual(errors, {})
            self.assertEqual(ok, {})

            # VSC_SCRATCH should also not do anything
            ok, errors = vo.process_vos(
                options, [test_vo_id], VSC_PRODUCTION_SCRATCH[BRUSSEL][0], mc, date, host_institute=BRUSSEL
            )
            self.assertEqual(errors, {})
            self.assertEqual(ok, {})

    @patch("vsc.accountpage.client.AccountpageClient", autospec=True)
    def test_process_brussel_default_vo_gent_user(self, mock_client):
        """Test for a vsc4 account in the default Brussel VO"""
        test_vo_id = "bvo00003"
        date = "203012310000"

        Options = namedtuple("Options", ["dry_run"])
        options = Options(dry_run=False)

        # first mock all the calls to the accountpage
        mc = mock_client.return_value
        mc.account = mock.MagicMock()
        mc.vo = mock.MagicMock()
        account_1 = {
            "vsc_id": "vsc40002",
            "status": "active",
            "isactive": True,
            "force_active": False,
            "expiry_date": None,
            "grace_until": None,
            "vsc_id_number": 2540002,
            "home_directory": "/user/gent/400/vsc40002",
            "data_directory": "/data/gent/400/vsc40002",
            "scratch_directory": "/scratch/gent/400/vsc40002",
            "login_shell": "/bin/bash",
            "broken": False,
            "email": "Stijn.DeWeirdt@UGent.be",
            "research_field": ["unknown", "unknown"],
            "create_timestamp": "2014-04-23T09:11:21.168000Z",
            "person": {
                "gecos": "Stijn Deweirdt",
                "institute": {"name": "gent"},
                "institute_login": "stdweird",
                "institute_affiliation": "unknown",
                "realeppn": "stdweird@UGent.be",
            },
            "home_on_scratch": False,
        }

        mc.account[account_1["vsc_id"]].get.return_value = (200, account_1)
        mc.vo[test_vo_id].member.modified[date].get.return_value = (200, [account_1])
        mc.vo[test_vo_id].get.return_value = (
            200,
            {
                "vsc_id": "bvo00003",
                "status": "active",
                "vsc_id_number": 2610008,
                "institute": {"name": "brussel"},
                "fairshare": 100,
                "data_path": "/data/brussel/vo/000/bvo00003",
                "scratch_path": "/scratch/brussel/vo/000/bvo00003",
                "description": "default gent VO at brussel",
                "members": [
                    "vsc40001",
                    "vsc40002",
                    "vsc40016",
                    "vsc40023",
                    "vsc40075",
                    "vsc40485",
                    "vsc41041",
                    "vsc41420",
                ],
                "moderators": [],
            },
        )
        mc.vo[test_vo_id].quota.get.return_value = (
            200,
            [
                {
                    "virtual_organisation": "bvo00003",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "bvo00003",
                    "hard": 104857600,
                }
            ],
        )
        mc.account[account_1["vsc_id"]].quota.get.return_value = (
            200,
            [
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_DELCATTY", "storage_type": "scratch"},
                    "fileset": "vsc400",
                    "hard": 262144000,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "vsc400",
                    "hard": 26214400,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_GENGAR", "storage_type": "scratch"},
                    "fileset": "vsc400",
                    "hard": 26214400,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_GULPIN", "storage_type": "scratch"},
                    "fileset": "vsc400",
                    "hard": 26214400,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_HOME", "storage_type": "home"},
                    "fileset": "vsc400",
                    "hard": 6291456,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_DELCATTY", "storage_type": "scratch"},
                    "fileset": "gvo00001",
                    "hard": 262144000,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_DATA", "storage_type": "data"},
                    "fileset": "gvo00001",
                    "hard": 52428800,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_GENGAR", "storage_type": "scratch"},
                    "fileset": "gvo00001",
                    "hard": 104857600,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "gent", "name": "VSC_SCRATCH_GULPIN", "storage_type": "scratch"},
                    "fileset": "gvo00001",
                    "hard": 104857600,
                },
                {
                    "user": "vsc40001",
                    "storage": {"institute": "brussel", "name": "VSC_SCRATCH_THEIA", "storage_type": "scratch"},
                    "fileset": "bvo00003",
                    "hard": 52428800,
                },
            ],
        )

        with patch('vsc.administration.base.StorageOperator') as mock_storage_operator:
            operator = mock.MagicMock()
            mock_storage_operator.return_value = operator

            # This shouldn't do anything
            ok, errors = vo.process_vos(options, [test_vo_id], VSC_HOME, mc, date, host_institute=BRUSSEL)
            self.assertEqual(errors, {})
            self.assertEqual(ok, {})

            # VSC_DATA should also not do anything
            ok, errors = vo.process_vos(options, [test_vo_id], VSC_DATA, mc, date, host_institute=BRUSSEL)
            self.assertEqual(errors, {})
            self.assertEqual(ok, {})

            # VSC_SCRATCH: this should allocate space
            ok, errors = vo.process_vos(
                options, [test_vo_id], VSC_PRODUCTION_SCRATCH[BRUSSEL][0], mc, date, host_institute=BRUSSEL
            )
            self.assertEqual(errors, {})
            self.assertEqual(ok, {'bvo00003': ['vsc40002']})


            operator().list_filesets.assert_called_with()
            operator().get_fileset_info.assert_called_with("theiascratch", "bvo00003")
            operator().chmod.assert_called_with(504, "/theia/scratch/brussel/vo/000/bvo00003")
            operator().chown.assert_called_with(99, 2610008, "/theia/scratch/brussel/vo/000/bvo00003")
            operator().set_fileset_quota.assert_called_with(
                102005473280, "/theia/scratch/brussel/vo/000/bvo00003", "bvo00003", 107374182400
            )
            operator().set_fileset_grace.assert_called_with("/theia/scratch/brussel/vo/000/bvo00003", 604800)
            operator().create_stat_directory.assert_called_with(
                "/theia/scratch/brussel/vo/000/bvo00003/vsc40002", 448, 2540002, 1, override_permissions=False
            )
