# -*- coding: utf-8 -*-

## Copyright (C) 2014, 2015 Bitergia
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
## Unit tests for family.scm.py (version 2, using Grimoire standard
##  testing libraries)
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB
from grimoirelib_alch.family.scm import (
    SCM,
    NomergesCondition, BranchesCondition, PeriodCondition,
    PersonsCondition
    )
from datetime import datetime
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'cp_cvsanaly_GrimoireLibTests'
schema_id = 'cp_cvsanaly_GrimoireLibTests'
start = datetime(2009,11,13)
end = datetime(2014,2,1)

class TestSCM (unittest.TestCase):

    def setUp (self):

        self.database = DB (url = url,
                            schema = schema,
                            schema_id = schema_id)
        self.session = self.database.build_session()
        self.start = start
        self.end = end


    def test_persons_condition (self):
        """Test SCM object with a persons condition"""

        persons = PersonsCondition (list_in = ("sbachenberg",
                                               "Chad Horohoe",
                                               "Evan Prodromou",
                                               "Sergey Chernyshev"),
                                    actors = "uauthors")
        data = SCM (datasource = self.database, name = "ncommits",
                    conditions = (persons,))
        self.assertEqual (data.total(), 90)

        persons = PersonsCondition (list_in = ("sbachenberg",
                                               "Chad Horohoe",
                                               "Evan Prodromou",
                                               "Sergey Chernyshev"),
                                    list_out = ("Chad Horohoe",
                                                "Evan Prodromou"),
                                    actors = "uauthors")
        data = SCM (datasource = self.database, name = "ncommits",
                    conditions = (persons,))
        self.assertEqual (data.total(), 40)

        persons = PersonsCondition (list_out = ("Chad Horohoe",
                                                "Evan Prodromou"),
                                    actors = "uauthors")
        data = SCM (datasource = self.database, name = "ncommits",
                    conditions = (persons,))
        self.assertEqual (data.total(), 1261)

        persons_in = PersonsCondition (list_in = ("sbachenberg",
                                               "Chad Horohoe",
                                               "Evan Prodromou",
                                               "Sergey Chernyshev"),
                                       actors = "uauthors")
        persons_out = PersonsCondition (list_out = ("Chad Horohoe",
                                                    "Evan Prodromou"),
                                        actors = "uauthors")
        data = SCM (datasource = self.database, name = "ncommits",
                    conditions = (persons_in, persons_out,))
        self.assertEqual (data.total(), 40)


if __name__ == "__main__":
    unittest.main()
