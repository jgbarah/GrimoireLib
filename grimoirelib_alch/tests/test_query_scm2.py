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
## Unit tests for scm_query.py (version 2, using Grimoire standard
##  testing libraries)
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB, Query
from grimoirelib_alch.type.timeseries import TimeSeries
from datetime import datetime
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'cp_cvsanaly_GrimoireLibTests'
schema_id = 'cp_cvsanaly_GrimoireLibTests'
start = datetime(2009,11,13)
end = datetime(2014,2,1)

class TestSCMQuery (unittest.TestCase):

    def setUp (self):
        database = DB (url = url,
                       schema = schema,
                       schema_id = schema_id)
        self.session = database.build_session(Query, echo = False)


    def test_filter_people (self):
        """Test select_people, filter_people"""
        
        correct = [
            [(6L, u'Siebrand Mazeland', u'siebrand@users.mediawiki.org'),
             (11L, u'Siebrand Mazeland', u's.mazeland@xs4all.nl'),
             ],
            [(12L, u'Chad Horohoe'),
             (13L, u'Evan Prodromou'),
             (14L, u'Andrew Garrett')
             ],
            [(6L,), (11L,)],
            ]

        res = self.session.query() \
            .select_people_fields(kind = "people",
                                  fields = ["id", "name", "email"]) \
            .filter_people (list_in = ["Siebrand Mazeland"],
                            kind = "people")
        self.assertEqual (res.all(), correct[0])

        res = res.filter_people (list_out = ["s.mazeland@xs4all.nl"],
                                 kind = "people", field = "email")
        self.assertEqual (res.all(), [correct[0][0],])

        res = self.session.query() \
            .select_people_fields(kind = "upeople",
                                  fields = ["id", "name", "email"]) \
            .filter_people (list_in = [12, 13, 14],
                            kind = "upeople", field ="id")
        self.assertEqual (res.all(), correct[1])

        res = res.filter_people (list_out = ["Chad Horohoe",
                                             "Andrew Garrett"],
                                 kind = "upeople", field = "name")
        self.assertEqual (res.all(), [correct[1][1],])

        res = self.session.query() \
            .select_people_fields(kind = "people",
                                  fields = ["id",]) \
            .filter_people (list_in = ["Siebrand Mazeland"],
                            kind = "people", field = "name")
        self.assertEqual (res.all(), correct[2])


    def test_filter_persons_id (self):
        """Test filter_persons_id"""
        
        correct = [1311, 40, 38, 37, 4]

        query = self.session.query() \
            .select_nscmlog(["commits",])
        self.assertEqual (query.scalar(), correct[0])
        query = query \
            .filter_persons_id (list_in = [13, 14, 15], kind = "authors")
        self.assertEqual (query.scalar(), correct[1])
        query = query \
            .filter_persons_id (list_out = [14,], kind = "authors")
        self.assertEqual (query.scalar(), correct[2])
        query = self.session.query() \
            .select_nscmlog(["commits",]) \
            .filter_persons_id (list_in = [13, 14, 15],
                                list_out = [14,],
                                kind = "authors")
        self.assertEqual (query.scalar(), correct[2])
        query = query.filter_nomerges()
        self.assertEqual (query.scalar(), correct[3])
        query = query.filter_period(start=start, end=end)
        self.assertEqual (query.scalar(), correct[4])


if __name__ == "__main__":
    unittest.main()
