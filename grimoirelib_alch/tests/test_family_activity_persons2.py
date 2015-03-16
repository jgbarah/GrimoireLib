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
## Unit tests for family.activity_persons
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB
from grimoirelib_alch.family.scm import (
    SCM, 
    PeriodCondition as SCMPeriodCondition,
    NomergesCondition as SCMNomergesCondition,
    OrgsCondition as SCMOrgsCondition,
    PersonsCondition as SCMPersonsCondition
    )
from grimoirelib_alch.family.activity_persons import SCMActivityPersons 
from datetime import datetime
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'cp_cvsanaly_GrimoireLibTests'
schema_id = 'cp_cvsanaly_GrimoireLibTests'
start = datetime(2013,11,13)
end = datetime(2014,2,1)


class TestActivityPersons (unittest.TestCase):

    def setUp (self):

        self.database = DB (url = url,
                            schema = schema,
                            schema_id = schema_id)
        self.session = self.database.build_session()
        self.start = start
        self.end = end


    def assertEqualStr (self, text1, text2):
        """Compare strings after removing all whitespace.

        Used to simplify comparison of strings.
        
        """

        self.assertEqual (''.join(text1.split()), ''.join(text2.split()))


    def test_persons_condition (self):
        """Test SCMActivityPersons object with a persons condition"""

        correct = [
"""
ActivityList:
{'period': Period, from 2011-12-16 13:13:12 to 2012-02-13 08:55:07, 'id': 1L, 'name': u'sbachenberg'}
{'period': Period, from 2012-09-26 14:50:08 to 2012-09-26 14:50:08, 'id': 10L, 'name': u'SBachenberg'}
{'period': Period, from 2014-03-17 22:25:34 to 2014-03-17 22:25:34, 'id': 12L, 'name': u'Chad Horohoe'}
{'period': Period, from 2006-07-25 17:34:55 to 2008-03-12 18:05:26, 'id': 13L, 'name': u'Evan Prodromou'}
{'period': Period, from 2009-02-24 22:26:02 to 2010-04-21 15:11:21, 'id': 21L, 'name': u'Sergey Chernyshev'}
{'period': Period, from 2009-04-06 17:26:44 to 2011-06-03 03:15:54, 'id': 22L, 'name': u'Chad Horohoe'}
"""
,
"""
ActivityList:
{'period': Period, from 2011-12-16 13:13:12 to 2012-09-26 14:50:08, 'id': 1L, 'name': u'sbachenberg'}
{'period': Period, from 2009-04-06 17:26:44 to 2014-03-17 22:25:34, 'id': 12L, 'name': u'Chad Horohoe'}
{'period': Period, from 2006-07-25 17:34:55 to 2008-03-12 18:05:26, 'id': 13L, 'name': u'Evan Prodromou'}
{'period': Period, from 2009-02-24 22:26:02 to 2010-04-21 15:11:21, 'id': 21L, 'name': u'Sergey Chernyshev'}
"""
,
"""
ActivityList:
{'period': Period, from 2014-01-11 22:12:01 to 2014-01-11 22:32:14, 'id': 6L, 'name': u'Siebrand Mazeland'}
{'period': Period, from 2013-11-14 20:36:26 to 2014-01-28 21:21:06, 'id': 9L, 'name': u'Translation updater bot'}
{'period': Period, from 2013-11-14 20:53:51 to 2013-12-02 20:37:04, 'id': 32L, 'name': u'Wikinaut'}
{'period': Period, from 2013-11-24 22:13:02 to 2013-11-24 22:13:02, 'id': 43L, 'name': u'Tyler Anthony Romeo'}
{'period': Period, from 2013-11-26 08:33:57 to 2013-12-15 23:35:28, 'id': 45L, 'name': u'Yuki Shira'}
{'period': Period, from 2013-12-18 23:04:11 to 2013-12-18 23:04:11, 'id': 49L, 'name': u'Alex Ivanov'}
"""
,
"""
ActivityList:
{'period': Period, from 2014-01-11 22:12:01 to 2014-01-11 22:32:14, 'id': 6L, 'name': u'Siebrand Mazeland'}
{'period': Period, from 2013-11-14 20:53:51 to 2013-12-02 20:37:04, 'id': 32L, 'name': u'Wikinaut'}
"""
]

        period = SCMPeriodCondition (start = self.start, end = self.end)
        nomerges = SCMNomergesCondition()
        orgs = SCMOrgsCondition (orgs = ("company1", "company2"),
                                 actors = "authors")
        persons_in = SCMPersonsCondition (list_in = ("sbachenberg",
                                                     "Chad Horohoe",
                                                     "Evan Prodromou",
                                                     "Sergey Chernyshev"),
                                       actors = "uauthors")
        persons_out = SCMPersonsCondition (list_out = ("Chad Horohoe",
                                                       "Evan Prodromou"),
                                           actors = "uauthors")
        data = SCMActivityPersons (
            datasource = self.session,
            name = "list_authors", conditions = (persons_in,))
        activity = data.activity()
        self.assertEqualStr (str(data.activity()), correct[0])

        data = SCMActivityPersons (
            datasource = self.session,
            name = "list_uauthors", conditions = (persons_in,))
        activity = data.activity()
        self.assertEqualStr (str(data.activity()), correct[1])

        data = SCMActivityPersons (
            datasource = self.session,
            name = "list_uauthors",
            conditions = (persons_out, period, nomerges)
            )
        activity = data.activity()
        self.assertEqualStr (str(data.activity()), correct[2])

        data = SCMActivityPersons (
            datasource = self.session,
            name = "list_uauthors",
            conditions = (persons_out, period, nomerges, orgs)
            )
        activity = data.activity()
        self.assertEqualStr (str(data.activity()), correct[3])

if __name__ == "__main__":
    unittest.main()
