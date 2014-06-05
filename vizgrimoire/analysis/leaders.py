#!/usr/bin/env python

# Copyright (C) 2014 Bitergia
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Authors:
#     Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#
#

import numpy as np

from analyses import Analyses

from query_builder import SCMQuery

from metrics_filter import MetricFilters

from scm_metrics import Commits

class Leaders(Analyses):
    # This class aims at being a meta-class that contains several analyses
    # on the importance of specific developers or organizations in the 
    # development of the source code.

    id = "leaders"
    name = "Leaders"
    desc = "Leaders of the project"


class SCMLeaders(Leaders):
    """ Class focuses on the development activity main actors (either organizations or developers)"""
    
    id = "main_developers"
    name = "Code Main Developers"
    desc = "Code main developers"

    def __init__(self, dbcon, filters, typeof_leader, repository=None, length=365):
        # dbcon: connection to the database
        # filters: specific filters
        #          type_analysis does not make sense in this case 
        # typeof_leader: "developer" or "organization" are accepted values
        # repository: this indicates the name of the repository. If value is None, 
        #             this forces the analysis taking into account all of the repositories
        # length: this variable indicates the period to analyze, by default the analysis is 
        # calculated using the last 365 days of history
        # In addition, usual filters from MetricFilters class are applied.
        #
        # This returns a list of organizations/developers with name, commits and percentage
        # That percentage represents the total number of commits per developer/organizations
        # out of the total number of commits with the limitations provided in filters.

        self.db = dbcon
        self.filters = filters
        self.typeof_leader = typeof_leader
        self.repository = repository
        self.length = length

    def _get_top_developers_sql(self):

        fields = """
                 select u.identifier as name, 
                        count(distinct(s.id)) as commits 
                 """
        tables = """
                 from actions a, 
                      scmlog s, 
                      people_upeople pup, 
                      upeople u 
                 """
        where = """
                where a.commit_id = s.id and 
                      s.author_id = pup.people_id and 
                      pup.upeople_id = u.id 
                """
        group = " group by u.id order by count(distinct(s.id)) desc limit " + str(self.filters.npeople)

        if self.repository is not None:
            tables = tables + ", repositories r "
            where = where + " and s.repository_id = r.id and r.name = " + self.repository

        query = fields + tables + where + group
        return query

    def _get_top_developers(self):
        developers = self.db.ExecuteQuery(self._get_top_developers_sql())
        print developers
        type(developers)
 
        if self.repository is None:
            self.filters.type_analysis = []
        else:
            self.filters.type_analysis = ["repository", self.repository]
        commits = Commits(self.db, self.filters)
        total_commits = commits.get_agg()

        developers_commits = np.array(developers['commits'])
        percentage_commits = (developers_commits / float(total_commits['commits'])) * 100

        developers['percentage'] = list(percentage_commits)
 
        return developers

    def _get_top_organizations(self):
        pass

    def result(self): 
        if self.typeof_leader == "developer":
            developers = self._get_top_developers()

        if self.typeof_leader == "organization":
            organizations = self._get_top_organizations()



# Examples of use
if __name__ == '__main__':
    filters = MetricFilters("week", "'2010-01-01'", "'2014-01-01'", ["repository", "'nova.git'"], 10)
    dbcon = SCMQuery("root", "", "dic_cvsanaly_openstack_2259", "dic_cvsanaly_openstack_2259",)
    leaders = SCMLeaders(dbcon, filters, "developer", "'nova.git'", 180)
    print leaders.result()

