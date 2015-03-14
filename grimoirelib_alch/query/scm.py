#! /usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2014 Bitergia
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
## Package to deal with queries for SCM data from *Grimoire
##  (CVSAnalY databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.type.timeseries import TimeSeries
from grimoirelib_alch.type.activity import ActivityList
from common import GrimoireDatabase, GrimoireQuery

from sqlalchemy import func, Column, Integer, ForeignKey, PrimaryKeyConstraint, or_
from sqlalchemy.sql import label
from datetime import datetime


class DB (GrimoireDatabase):
    """Class for dealing with SCM (CVSAnalY) databases.

    """
 
    def _query_cls(self):
        """Return que defauld Query class for this database

        Returns
        -------

        GrimoireQuery: default Query class.

        """

        return Query

    def _create_tables(self, tables = None, tables_id = None):
        """Create all SQLAlchemy tables.

        Builds a SQLAlchemy class per SQL table, by using _table().
        It assumes self.Base, self.schema and self.schema_id are already
        set (see super.__init__() code).

        """

        DB.Actions = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Actions',
            tablename = 'actions',
            schemaname = self.schema,
            columns = dict (
                commit_id = Column(Integer,
                                   ForeignKey(self.schema + '.' + 'scmlog.id')
                                   ),
                branch_id = Column(Integer,
                                   ForeignKey(self.schema + '.' + 'branches.id')
                                   ),
                file_id = Column(Integer,
                                 ForeignKey(self.schema + '.' + 'files.id')
                                 )
                ))
        DB.Branches = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Branches',
            tablename = 'branches',
            schemaname = self.schema
            )
        DB.Files = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Files',
            tablename = 'files',
            schemaname = self.schema,
            columns = dict (
                repository_id = Column(Integer,
                                       ForeignKey(self.schema + '.' + 
                                                  'repositories.id')
                                       )
                ))
        DB.FileLinks = GrimoireDatabase._table (
            bases = (self.Base,), name = 'FileLinks',
            tablename = 'file_links',
            schemaname = self.schema,
            columns = dict (
                file_id = Column(Integer,
                                 ForeignKey(self.schema + '.' + 'files.id')
                                 )
                ))
        DB.People = GrimoireDatabase._table (
            bases = (self.Base,), name = 'People',
            tablename = 'people',
            schemaname = self.schema
            )
        DB.PeopleUPeople = GrimoireDatabase._table (
            bases = (self.Base,),
            name = 'PeopleUPeople',
            tablename = 'people_upeople',
            schemaname = self.schema,
            columns = dict (
                upeople_id = Column(
                    Integer,
                    ForeignKey(self.schema_id + '.' + 'upeople.id'),
                    primary_key = True
                    ),
                people_id = Column(
                    Integer,
                    ForeignKey(self.schema + '.' + 'people.id'),
                    primary_key = True
                    ),
                )
            )
        # DB.PeopleUPeople.__table_args__ = (
        #     PrimaryKeyConstraint(DB.PeopleUPeople.upeople_id,
        #                          DB.PeopleUPeople.people_id),)
        
        DB.Repositories = GrimoireDatabase._table (
            bases = (self.Base,),
            name = 'Repositories',
            tablename = 'repositories',
            schemaname = self.schema,
            )
        DB.SCMLog = GrimoireDatabase._table (
            bases = (self.Base,), name = 'SCMLog',
            tablename = 'scmlog',
            schemaname = self.schema,
            columns = dict (
                author_id = Column(Integer,
                                   ForeignKey(self.schema_id + '.' + 'people.id')
                                   ),
                committer_id = Column(Integer,
                                      ForeignKey(self.schema_id + '.' + 'people.id')
                                      ),
                repository_id = Column(Integer,
                                       ForeignKey(self.schema_id + '.' + 
                                                  'repositories.id')
                                       ),
                ))
        DB.UPeople = GrimoireDatabase._table (
            bases = (self.Base,), name = 'UPeople',
            tablename = 'upeople',
            schemaname = self.schema_id)

        if "companies" in tables_id:
            DB.Companies = GrimoireDatabase._table (
                bases = (self.Base,), name = 'Companies',
                tablename = 'companies',
                schemaname = self.schema_id,
                columns = dict (
                    id = Column(Integer, primary_key = True)
                    )
                )

        if "upeople_companies" in tables_id:
            DB.UPeopleCompanies = GrimoireDatabase._table (
                bases = (self.Base,), name = 'UPeopleCompanies',
                tablename = 'upeople_companies',
                schemaname = self.schema_id,
                )

class Query (GrimoireQuery):
    """Class for dealing with SCM queries"""


    def select_nscmlog(self, variables):
        """Select a column which is a field in SCMLog.

        Parameters
        ----------

        variables: { "commits", "authors", "committers" }
           Columns (variables) to select

        Returns
        -------

        Query object

        """

        if not isinstance(variables, (list, tuple)):
            raise Exception ("select_nscmlog: Argument is not list or tuple")
        elif len (variables) == 0:
            raise Exception ("select_nscmlog: No variables")
        fields = []
        if DB.SCMLog not in self.joined:
            self.joined.append(DB.SCMLog)
        for variable in variables:
            if variable == "commits":
                name = "nocommits"
                field = DB.SCMLog.rev
            elif variable == "authors":
                name = "nauthors"
                field = DB.SCMLog.author_id
            elif variable == "committers":
                name = "ncommitters"
                field = DB.SCMLog.committer_id
            else:
                raise Exception ("select_nscmlog: Unknown variable %s." \
                                     % variable)
            fields.append (label (name,
                                  func.count(func.distinct(field))))
        return self.add_columns (*fields)

    def select_listcommits(self):
        """Select a list of commits"""
        
        if DB.SCMLog not in self.joined:
            self.joined.append(DB.SCMLog)
        return self \
            .add_columns (label("id", func.distinct(DB.SCMLog.id)),
                          label("date", DB.SCMLog.date))

    def select_listpersons_uid(self, kind = "all"):
        """Select a list of persons (authors, committers), using uids

        Parameters
        ----------
        
        kind: { "authors", "committers", "all" }
           Kind of person to select: authors (authors of commits),
           committers (committers of commits),
           all (authors and committers)
          
        Returns
        -------

        Query object: with (id, name, email) selected.

        """
        
        query = self.add_columns (label("id", func.distinct(DB.UPeople.id)),
                                  label("name", DB.UPeople.identifier)) \
                .join (DB.PeopleUPeople,
                       DB.UPeople.id == DB.PeopleUPeople.upeople_id)
        if kind == "authors":
            return query \
                .join (DB.SCMLog,
                       DB.PeopleUPeople.people_id == DB.SCMLog.author_id)
        elif kind == "committers":
            return query \
                .join (DB.SCMLog,
                       DB.PeopleUPeople.people_id == DB.SCMLog.committer_id)
        elif kind == "all":
            return query \
                .join (DB.SCMLog,
                       DB.PeopleUPeople.people_id == DB.SCMLog.author_id or
                       DB.PeopleUPeople.people_id == DB.SCMLog.committer_id)
        else:
            raise Exception ("select_listpersons_uid: Unknown kind %s." \
                             % kind)

    def select_listpersons(self, kind = "all"):
        """Select a list of persons (authors, committers)

        - kind: kind of person to select
           authors: authors of commits
           committers: committers of commits
           all: authors and committers

        Returns a Query object, with (id, name, email) selected.
        """

        query = self.add_columns (label("id", func.distinct(DB.People.id)),
                                  label("name", DB.People.name),
                                  label('email', DB.People.email))
        if kind == "authors":
            return query \
                .join (DB.SCMLog, DB.People.id == DB.SCMLog.author_id)    
        elif kind == "committers":
            return query \
                .join (DB.SCMLog, DB.People.id == DB.SCMLog.committer_id)    
        elif kind == "all":
            return query \
                .join (DB.SCMLog, DB.People.id == DB.SCMLog.author_id or
                       DB.People.id == DB.SCMLog.committer_id)
        else:
            raise Exception ("select_listpersons: Unknown kind %s." \
                             % kind)

    def select_personsdata(self, kind):
        """Adds columns with persons data to select clause.

        Adds people.name, people.email to the select clause of query.
        Does not join new tables.

        Parameters
        ----------

        kind: {"authors", "committers"}
           Kind of person to select

        Returns
        -------

        SCMObject: Result query, with new fields: id, name, email        

        """

        query = self.add_columns (label("person_id", DB.People.id),
                                  label("name", DB.People.name),
                                  label('email', DB.People.email))
        if kind == "authors":
            person = DB.SCMLog.author_id
        elif kind == "committers":
            person = DB.SCMLog.committer_id
        else:
            raise Exception ("select_personsdata: Unknown kind %s." \
                             % kind)
        if DB.SCMLog in self.joined:
            query = query.filter (DB.People.id == person)
        else:
            self.joined.append (DB.SCMLog)
            query = query.join (DB.SCMLog, DB.People.id == person)
        return query


    def select_personsdata_uid(self, kind):
        """Adds columns with persons data to select clause (uid version).

        Adds person_id, name, to the select clause of query, having unique
        identities into account.
        Joins with PeopleUPeople, UPeople, SCMLog if they are not
        already joined.
        Relationships: UPeople.id == PeopleUPeople.upeople_id,
        PeopleUPeople.people_id == person

        Parameters
        ----------

        kind: {"authors", "committers"}
           Kind of person to select

        Returns
        -------

        SCMObject: Result query, with two new fields: id, name

        """

        if kind == "authors":
            person = DB.SCMLog.author_id
        elif kind == "committers":
            person = DB.SCMLog.committer_id
        else:
            raise Exception ("select_personsdata_uid: Unknown kind %s." \
                             % kind)
        query = self.add_columns (label("person_id", DB.UPeople.id),
                                  label("name", DB.UPeople.identifier))
        if not self.joined:
            # First table, UPeople is in FROM
            self.joined.append (DB.UPeople)
        if not self.joined or DB.UPeople in self.joined:
            # First table, UPeople is in FROM, or we have UPeople
            if DB.PeopleUPeople not in self.joined:
                self.joined.append (DB.PeopleUPeople)
                query = query.join (
                    DB.PeopleUPeople,
                    DB.UPeople.id == DB.PeopleUPeople.upeople_id
                    )
            if DB.SCMLog not in self.joined:
                self.joined.append (DB.SCMLog)
                query = query.join (DB.SCMLog,
                                    DB.PeopleUPeople.people_id == person)
        elif DB.PeopleUPeople in self.joined:
            # We have PeopleUPeople (SCMLog should be joined), no UPeople
            if DB.SCMLog not in self.joined:
                raise Exception ("select_personsdata_uid: " + \
                                     "If PeopleUPeople is joined, " + \
                                     "SCMLog should be joined too")
            self.joined.append (DB.UPeople)
            query = query.join (DB.UPeople,
                                DB.UPeople.id == DB.PeopleUPeople.upeople_id)
        elif DB.SCMLog in self.joined:
            # We have SCMLog, and no PeopleUPeople, no UPeople
            self.joined.append (DB.PeopleUPeople)
            query = query.join (DB.PeopleUPeople,
                                DB.PeopleUPeople.people_id == person)
            self.joined.append (DB.UPeople)
            query = query.join (DB.UPeople,
                                DB.UPeople.id == DB.PeopleUPeople.upeople_id)
        else:
            # No SCMLog, no PeopleUPeople, no UPeople but some other table
            raise Exception ("select_personsdata_uid: " + \
                                 "Unknown table to join to")
        return query


    def select_people_fields(self, kind, fields = ["id"]):
        """Adds fields from the people or upeople table to a query.

        Parameters
        ----------
        kind: {"people" | "upeople"}
           Kind of table to select from.
        fields: list of str
           List of strings with the names of the fields to select
           ("id": people.id or upeople.id, "name": people.name or
           upeople.identifier, "email": people.email).
           Default: "id".

        """

        query = self
        if kind == "people":
            if DB.People not in self.joined:
                self.joined.append(DB.People)
            if "id" in fields:
                query = query.add_columns (label("person_id",
                                                 DB.People.id))
            if "name" in fields:
                query = query.add_columns (label("name",
                                                 DB.People.name))
            if "email" in fields:
                query = query.add_columns (label("email",
                                                 DB.People.email))
        elif kind == "upeople":
            if DB.UPeople not in self.joined:
                self.joined.append(DB.UPeople)
            if "id" in fields:
                query = query.add_columns (label("person_id",
                                                DB.UPeople.id))
            if "name" in fields:
                query = query.add_columns (label("name",
                                                DB.UPeople.identifier))
        else:
            raise Exception ("select_people_fields: Unknown kind %s." \
                                 % kind)
        return query

    def select_commitsperiod(self):
        """Add to select the period of the selected commits.

        Adds min(scmlog.date) and max(scmlog.date) for selected commits.
        
        Returns
        -------

        SCMObject: Result query, with two new fields: firstdate, lastdate

        """

        query = self.add_columns (label('firstdate',
                                        func.min(DB.SCMLog.date)),
                                  label('lastdate',
                                        func.max(DB.SCMLog.date)))
        return query
    
    def select_nbranches(self):
        """Select number of branches in repo.

        The Actions table is used, instead of the Branches table,
        so that some filters, such as filter_period() can be used
        """

        query = self.add_columns (
            label("nbranches",
                  func.count(func.distinct(DB.Actions.branch_id)))
            )
        return query

    def select_nfiles(self):
        """Select number of files in repo.

        The Actions table is used because we just want the number of
        files, considering eg. a file moved to a new pathname as the
        same file.

        Returns
        -------

        Query object

        """

        query = self.add_columns (
            label("nfiles",
                  func.count(func.distinct(DB.Actions.file_id)))
            )
        if DB.Actions not in self.joined:
            self.joined.append (DB.Actions)
        return query

    def select_listbranches(self):
        """Select list of branches in repo

        Returns a list with a tuple (id, name) per branch
        The Actions table is used, instead of the Branches table,
        so that some filters, such as filter_period() can be used
        """

        query = self.add_columns (label("id",
                                        func.distinct(DB.Actions.branch_id)),
                                  label("name",
                                        DB.Branches.name))
        query = query.join(DB.Branches)
        return query

    def select_repos (self, count = False, distinct = False, names = False):
        """Select repositories data for each commit

        Include repository ids and names (if names is True)
        as columns in SELECT. The new columns are "repo" (id) and "name".
        If distinct is True, select distinct repository ids.
        If count is True, select the number of repositoy ids.

        Parameters
        ----------

        count: bool
           Produce count of repositories instead of list
        distinct: bool
           Select distinct repository ids.
        names: bool
           Include names of repositories as a column (or not).

        Returns
        -------

        Query
            Including new columns in SELECT
        
        """

        id_field = DB.SCMLog.repository_id
        if distinct:
            id_field = func.distinct(id_field)
        if count:
            id_field = func.count(id_field)
        query = self.add_columns (label("repo", id_field))
        if names:
            query = query.add_columns (label("name", DB.Repositories.name))
            if DB.Repositories not in self.joined:
                self.joined.append (DB.Repositories)
                query = query.join (
                    DB.Repositories,
                    DB.SCMLog.repository_id == DB.Repositories.id)
        return query

    def select_orgs (self):
        """Select organizations data.

        Include id and name, as they appear in the companies table.
        Warning: doesn't join other tables. For now, only works alone.

        Returns
        -------

        Query
            Including new columns in SELECT
        
        """

        query = self.add_columns (label("org_id", DB.Companies.id),
                                  label("org_name", DB.Companies.name))
        return query


    def filter_nomerges (self):
        """Consider only commits that touch files (no merges)

        For considering only those commits, join with Actions,
        where merge commits are not represented.
        """

        if DB.Actions not in self.joined:
            self.joined.append (DB.Actions)
            return self.join(DB.Actions)
        else:
            return self

    def filter_branches (self, branches):
        """Filter variables for a set of branches

        - branches (list of string): list of branches to consider

        Returns the object query, extended with the filter for branches.
        """

        query = self
        if DB.Actions not in self.joined:
            self.joined.append (DB.Actions)
            query = query.join(DB.Actions)
        if DB.Branches not in self.joined:
            self.joined.append (DB.Branches)
            query = query.join(DB.Branches)
        query = query.filter(DB.Branches.name.in_(branches))
        return query


    def filter_period(self, start = None, end = None, date = "commit"):
        """Filter variable for a period

        - start: datetime, starting date
        - end: datetime, end date
        - date: "commit" or "author"
            Git maintains "commit" and "author" date

        Commits considered are between starting date and end date
        (exactly: start <= date < end)
        """

        query = self
        if date == "author":
            scmlog_date = DB.SCMLog.author_date
        elif date == "commit":
            scmlog_date = DB.SCMLog.date
        if start is not None:
            self.start = start
            query = query.filter(scmlog_date >= start.isoformat())
        if end is not None:
            self.end = end
            query = query.filter(scmlog_date < end.isoformat())
        return query


    def filter_persons (self, list, kind = "all"):
        """Fiter for a certain list of persons (committers, authors)

        Parameters
        ----------

        list: list of People.id
           List of people to include
        kind: {'committers', 'authors', 'all'}
           kind of person to select (all: authors and committers)

        Returns
        -------
        
        Query object.

        """

        query = self
        if kind == "authors":
            return query.filter (DB.SCMLog.author_id.in_(list))    
        elif kind == "committers":
            return query.filter (DB.SCMLog.committer_id.in_(list))    
        elif kind == "all":
            return query.filter (or_(DB.SCMLog.author_id.in_(list),
                                     DB.SCMLog.committer_id.in_(list)))
        else:
            raise Exception ("filter_persons: Unknown kind %s." \
                             % kind)


    def filterout_persons (self, list, kind = "all"):
        """Filter out a list of people ids (committers, authors)

        Parameters
        ----------

        Parameters
        ----------

        list: list of People.id
           List of people to exclude
        kind: {'committers', 'authors', 'all'}
           kind of person to select (all: authors and committers)

        Returns
        -------
        
        Query object.

        """

        query = self
        if kind == "authors":
            return query.filter (~DB.SCMLog.author_id.in_(list))    
        elif kind == "committers":
            return query.filter (~DB.SCMLog.committer_id.in_(list))    
        elif kind == "all":
            return query.filter (and_(~DB.SCMLog.author_id.in_(list),
                                     ~DB.SCMLog.committer_id.in_(list)))
        else:
            raise Exception ("filterout_persons: Unknown kind %s." \
                             % kind)


    def filter_persons_id (self, list_in = None, list_out = None,
                           kind = "all"):
        """Fiter for a certain list of persons (committers, authors).

        Parameters
        ----------

        list_in: list of str
           List of people ids to include (only this will be included).
           Default: None, all are included.
        list_out: list of str
           List of people ids to exclude (these will not be included).
           Default: None, no one is included.
        kind: {'committers' | 'authors' | 'all'}
           kind of person to select (all: authors and committers)

        Returns
        -------
        
        Query object.

        """

        query = self
        if kind not in ("authors", "committers", "all"):
            raise Exception ("filter_persons: Unknown kind %s." \
                                 % kind)
        if list_in is not None:
            if kind == "authors":
                query = query.filter (DB.SCMLog.author_id.in_(list_in))    
            elif kind == "committers":
                query =  query.filter (DB.SCMLog.committer_id.in_(list_in))
            elif kind == "all":
                query = query.filter (
                    or_(DB.SCMLog.author_id.in_(list_in),
                        DB.SCMLog.committer_id.in_(list_in))
                    )
        if list_out is not None:
            if kind == "authors":
                query = query.filter (~DB.SCMLog.author_id.in_(list_out))
            elif kind == "committers":
                query = query.filter (~DB.SCMLog.committer_id.in_(list_out))
            elif kind == "all":
                query = query.filter (
                    and_(~DB.SCMLog.author_id.in_(list_out),
                        ~DB.SCMLog.committer_id.in_(list_out))
                    )
        return query


    def filter_paths (self, list):
        """Fiter for a certain list of paths

        Parameters
        ----------

        list: list of strings
          Prefixes of paths to filter

        Returns
        -------

        Query object.

        """

        conditions = []
        for path in list:
            condition = DB.FileLinks.file_path.like(path + '%')
            conditions.append(condition)
        query = self
        if DB.Actions not in self.joined:
            self.joined.append (DB.Actions)
            query = query.join(DB.Actions,
                               DB.SCMLog.id == DB.Actions.commit_id)
        if DB.FileLinks not in self.joined:
            self.joined.append (DB.FileLinks)
            query = query \
                .join(DB.FileLinks, DB.Actions.file_id == DB.FileLinks.file_id)
        else:
            query = query.filter(DB.Actions.file_id == DB.FileLinks.file_id)
        query = query.filter(or_(*conditions))
        return query


    def filter_people (self, list_in = None, list_out = None,
                       kind = "people", field = "name"):
        """Filter from a people or upeople table.

        It assumes people or upeople tables are already joined.

        Parameters
        ----------

        list_in: list of str
           List of people to include (only this will be included).
           Default: None, all are included.
        list_out: list of str
           List of people to exclude (these will not be included).
           Default: None, no one is included.
        kind: {"people" | "upeople" }
           Kind of table to use to filter in or out (people table,
           or upeople table for "unique" identifiers).
           Default: "people"
        field: str
           Name of the fields to use for filtering
           ("id": people.id or upeople.id, "name": people.name or
           upeople.identifier, "email": people.email).
           Default: "name".

        Returns
        -------
        
        Query object.

        """

        if kind == "people":
            table = DB.People
        elif kind == "upeople":
            table = DB.UPeople
        else:
            raise Exception ("filter_person_names: Unknown kind %s." % kind)
        if table not in self.joined:
            raise Exception ("filter_person_names: " \
                                 + kind + " table not joined")
        query = self
        if field == "id":
            filter_field = table.id
        elif field == "name":
            if table == DB.People:
                filter_field = DB.People.name
            else:
                filter_field = DB.UPeople.identifier
        elif field == "email":
            if table == DB.People:
                filter_field = DB.People.email
            else:
                raise Exception ("filter_person_names: " \
                                     + "no email in upeople table.")
        if list_in is not None:
            query = query.filter (filter_field.in_(list_in))
        if list_out is not None:
            query = query.filter (~filter_field.in_(list_out))
        return query


    def filter_orgs (self, orgs):
        """Filter organizations matching a list of names

        Fiters query by a list of organization names, checking for
        them in the companies table.

        Parameters
        ----------
        
        orgs: list of str
            List of organizations

        """

        query = self
        query = query.filter(DB.Companies.name.in_(orgs))
        return query

    def filter_org_ids (self, list, kind = "authors"):
        """Filter organizations matching a list of organization ids

        Fiters query by a list of organization ids.

        Parameters
        ----------
        
        list: list of int
            List of organization ids
        kind: {"authors", "committers"}
            Kind of actor to consider

        """

        query = self
        if kind == "authors":
            person_id = DB.SCMLog.author_id
            date_id = DB.SCMLog.author_date
        elif kind == "committers":
            person_id = DB.SCMLog.committer_id
            date_id = DB.SCMLog.date
        else:
            raise Exception ("filter_org_ids: Unknown kind %s." \
                             % kind)
        query = query \
            .join (DB.PeopleUPeople,
                   person_id == DB.PeopleUPeople.people_id) \
            .join (DB.UPeopleCompanies,
                   DB.PeopleUPeople.upeople_id == \
                       DB.UPeopleCompanies.upeople_id) \
            .filter (date_id.between (
                           DB.UPeopleCompanies.init,
                           DB.UPeopleCompanies.end
                           )) \
            .filter (DB.UPeopleCompanies.company_id.in_(list))
        return query

    def group_by_period (self):
        """Group by time period (per month)"""

        return self \
            .add_columns (label("month", func.month(DB.SCMLog.date)),
                          label("year", func.year(DB.SCMLog.date))) \
            .group_by("month", "year").order_by("year", "month")


    def group_by_person (self):
        """Group by person

        Uses person_id field in the query to do the grouping.
        That field should be added by some other method.

        Parameters
        ----------

        None

        Returns
        -------

        Query object, with a new field (person_id)
        and a "group by" clause for grouping the results.

        """

        return self.group_by("person_id")

    def group_by_repo (self, names = False):
        """Group by repository

        Include repository ids and names (if names is True)
        as columns in SELECT, grouping by repository ids. The new
        columns are "repo" (id) and "name".

        Parameters
        ----------

        names: Boolean
           Include names of repositories as a column (or not).
        
        Returns
        -------

        Query: with new new columns in SELECT
        
        """

        query = self.select_repos (names = names)
        query = query.group_by("repo").order_by("repo")
        return query

    def timeseries (self):
        """Return a TimeSeries object.

        The query has to include a group_by_period filter.

        """

        data = []
        for row in self.all():
            # Extract real values (entries which are not year or month)
            values = tuple(row[i] for i, k in enumerate(row.keys())
                           if not k in ("year", "month"))
            data.append ((datetime (row.year, row.month, 1),
                         values))
        return TimeSeries (period = "months",
                           start = self.start, end = self.end,
                           data = data)

    def activity (self):
        """Return an ActivityList object.

        The query has to produce rows with the following fields:
        id (string),  name (string), start (datetime), end (datetime)

        """

        list = self.all()
        return ActivityList(list)


if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()

    database = DB (url = 'mysql://jgb:XXX@localhost/',
                   schema = 'vizgrimoire_cvsanaly',
                   schema_id = 'vizgrimoire_cvsanaly')
    session = database.build_session(Query, echo = False)

    #---------------------------------
    print_banner ("Number of commits")
    res = session.query().select_nscmlog(["commits",]) \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    res = session.query().select_nscmlog(["commits",]) \
        .filter_nomerges() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    res = session.query().select_nscmlog(["commits",]) \
        .filter_period(end=datetime(2014,1,1))
    print res.scalar()

    #---------------------------------
    print_banner("Number of commits, grouped by authors")
    res = session.query().select_nscmlog(["commits",]) \
        .select_personsdata("authors") \
        .group_by_person()
    for row in res.limit(10).all():
        print row.person_id, row.nocommits

    #---------------------------------
    print_banner("Number of commits, grouped by authors, \n" +
                 "including data per author")
    res = session.query().select_nscmlog(["commits",]) \
        .select_personsdata("authors") \
        .group_by_person()
    for row in res.limit(10).all():
        print row.nocommits, row.person_id, row.name, row.email

    #---------------------------------
    print_banner("Number of commits, grouped by authors, including data\n" +
                 "and period of activity per author")
    res = session.query().select_nscmlog(["commits",]) \
        .select_personsdata("authors") \
        .select_commitsperiod() \
        .group_by_person()
    for row in res.order_by("nocommits desc").limit(10).all():
        print row.nocommits, row.person_id, row.name, row.email, \
            row.firstdate, row.lastdate

    #---------------------------------
    print_banner("Number of commits, grouped by authors, including data\n" +
                 "and period of activity per author, for a certain period")
    res = session.query().select_nscmlog(["commits",]) \
        .select_personsdata("authors") \
        .select_commitsperiod() \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1)) \
        .group_by_person()
    for row in res.order_by("nocommits desc").limit(10).all():
        print row.person_id, row.nocommits, row.name, row.email, \
            row.firstdate, row.lastdate

    #---------------------------------
    print_banner("Number of commits, grouped by authors, including data\n" +
                 "and period of activity per author (uid version)")
    res = session.query().select_nscmlog(["commits",]) \
        .select_personsdata_uid("authors") \
        .select_commitsperiod() \
        .group_by_person()

    for row in res.order_by("nocommits desc").limit(10).all():
        print row.nocommits, row.person_id, row.name, \
            row.firstdate, row.lastdate

    #---------------------------------
    print_banner("Data and period of activity per author, " + \
                     "for a certain period")
    res = session.query() \
        .select_personsdata("authors") \
        .select_commitsperiod() \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1)) \
        .group_by_person()
    for row in res.order_by("firstdate").limit(10).all():
        print row.person_id, row.name, row.email, \
            row.firstdate, row.lastdate

    #---------------------------------
    print_banner("Activity list (authors, for a certain period)")
    print res.activity()

    print_banner("Time series of commits")
    res = session.query().select_nscmlog(["commits",]) \
        .group_by_period() \
        .filter_period(end=datetime(2014,1,1))
    ts = res.timeseries ()
    print (ts)

    #---------------------------------
    print_banner("Data and period of activity per author, " + \
                     "for a certain period, no merge commits.")
    res = session.query() \
        .select_personsdata("authors") \
        .select_commitsperiod() \
        .filter_nomerges() \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1)) \
        .group_by_person()

    for row in res.order_by("firstdate").limit(10).all():
        print row.person_id, row.name, row.email, \
            row.firstdate, row.lastdate

    #---------------------------------
    print_banner("Data and period of activity per author, " + \
                     "for a certain period, no merge commits " + \
                     "(uid version).")
    res = session.query() \
        .select_personsdata_uid("authors") \
        .select_commitsperiod() \
        .filter_nomerges() \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1)) \
        .group_by_person()

    for row in res.order_by("firstdate").limit(10).all():
        print row.person_id, row.name, row.firstdate, row.lastdate

    #---------------------------------
    print_banner("List of commits")
    res = session.query() \
        .select_listcommits() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    for row in res.limit(10).all():
        print row.id, row.date

    #---------------------------------
    print_banner("Number of authors")
    res = session.query().select_nscmlog(["authors",]) \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    
    #---------------------------------
    print_banner("List of authors")
    resAuth = session.query() \
        .select_listpersons("authors") \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1))
    for row in resAuth.limit(10).all():
        print row.id, row.name
    
    #---------------------------------
    print_banner("Filter master branch")
    res = res.filter_branches(("master",))
    print res.all()

    #---------------------------------
    print_banner("List of branches")
    res = session.query().select_listbranches()
    print res.all()
    res = session.query().select_listbranches() \
        .join(DB.SCMLog) \
        .filter_period(start=datetime(2013,12,1),
                       end=datetime(2014,2,1))
    print res.all()

    #---------------------------------
    print_banner("List of authors, filter some paths")
    res = resAuth.filter_paths(("examples",))
    print res.all()

    
