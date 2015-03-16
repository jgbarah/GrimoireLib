# -*- coding: utf-8 -*-

## Copyright (C) 2015 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## Unit tests for family.activity_timezones
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB as SCMDatabase
from grimoirelib_alch.family.scm import (
    PeriodCondition as SCMPeriodCondition,
    NomergesCondition as SCMNomergesCondition,
    PersonsCondition as SCMPersonsCondition
    )
from grimoirelib_alch.family.activity_timezones import SCMActivityTZ
from datetime import datetime
import unittest

url = 'mysql://jgb:XXX@localhost/'
#schema = 'cp_cvsanaly_GrimoireLibTests'
#schema_id = 'cp_cvsanaly_GrimoireLibTests'
schema = 'oscon_openstack_scm'
schema_id = 'oscon_openstack_scm'
start = datetime(2013,11,13)
end = datetime(2014,2,1)

class TestActivityPersons (unittest.TestCase):

    def setUp (self):

        self.scm_database = SCMDatabase (
            url = url,
            schema = schema,
            schema_id = schema_id
            )
        self.session = self.scm_database.build_session()
        self.start = start
        self.end = end


    def test_scm_tz (self):
        """Test SCMActivityTZ object with a persons condition"""

        correct = [
            {'commits': [586L, 829L, 61L, 0, 6327L, 15163L, 3585L, 10720L, 9161L, 392L, 59L, 0, 24564L, 7480L, 6385L, 967L, 2930L, 882L, 0, 8L, 4293L, 1988L, 1585L, 869L],
             'tz': [-12L, -11L, -10L, -9, -8L, -7L, -6L, -5L, -4L, -3L, -2L, -1, 0L, 1L, 2L, 3L, 4L, 5L, 6, 7L, 8L, 9L, 10L, 11L],
             'authors': [5L, 9L, 3L, 0, 376L, 620L, 253L, 568L, 413L, 59L, 13L, 0, 604L, 386L, 400L, 105L, 127L, 150L, 0, 3L, 317L, 114L, 53L, 28L]},
            {'commits': [0, 148L, 1L, 0, 1123L, 224L, 560L, 1142L, 54L, 48L, 14L, 0, 2970L, 1065L, 311L, 16L, 319L, 147L, 0, 2L, 768L, 149L, 101L, 122L],
             'tz': [-12, -11L, -10L, -9, -8L, -7L, -6L, -5L, -4L, -3L, -2L, -1, 0L, 1L, 2L, 3L, 4L, 5L, 6, 7L, 8L, 9L, 10L, 11L],
             'authors': [0, 5L, 1L, 0, 123L, 69L, 73L, 145L, 22L, 12L, 3L, 0, 146L, 124L, 74L, 10L, 43L, 31L, 0, 1L, 100L, 24L, 11L, 16L]},
            {'commits': [0, 148L, 1L, 0, 1123L, 224L, 560L, 1142L, 54L, 48L, 14L, 0, 1016L, 1065L, 311L, 16L, 319L, 147L, 0, 2L, 768L, 149L, 101L, 122L],
             'tz': [-12, -11L, -10L, -9, -8L, -7L, -6L, -5L, -4L, -3L, -2L, -1, 0L, 1L, 2L, 3L, 4L, 5L, 6, 7L, 8L, 9L, 10L, 11L],
             'authors': [0, 5L, 1L, 0, 123L, 69L, 73L, 145L, 22L, 12L, 3L, 0, 144L, 124L, 74L, 10L, 43L, 31L, 0, 1L, 100L, 24L, 11L, 16L]}
            ]

        scm_tz = SCMActivityTZ (
            datasource = self.scm_database,
            name = "authors"
            )
        tz = scm_tz.timezones()
        self.assertEqual (tz, correct[0])

        period = SCMPeriodCondition (start = self.start, end = self.end)
        nomerges = SCMNomergesCondition()
        scm_tz = SCMActivityTZ (
            datasource = self.scm_database,
            name = "authors",
            conditions = (period,nomerges)
            )
        tz = scm_tz.timezones()
        self.assertEqual (tz, correct[1])

        persons_out = SCMPersonsCondition (
            list_out = ("Jenkins",
                        "OpenStack Jenkins",
                        "Launchpad Translations on behalf of nova-core",
                        "Jenkins",
                        "OpenStack Hudson",
                        "gerrit2@review.openstack.org",
                        "linuxdatacenter@gmail.com",
                        "Openstack Project Creator",
                        "Openstack Gerrit",
                        "openstackgerrit",
                        "OpenStack Proposal Bot",
                        "Owl Bot"),
            actors = "uauthors")
        scm_tz = SCMActivityTZ (
            datasource = self.scm_database,
            name = "authors",
            conditions = (period,nomerges, persons_out)
            )
        tz = scm_tz.timezones()
        print tz
        self.assertEqual (tz, correct[2])
 
if __name__ == "__main__":
    unittest.main()
