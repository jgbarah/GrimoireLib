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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##


import logging

from datetime import datetime

from GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff
from GrimoireSQL import ExecuteQuery

from metrics import Metrics

from MLS import MLS


class EmailsSent(Metrics):
    """ Emails metric class for mailing lists analysis """

    id = "sent"
    name = "Emails Sent"
    desc = "Emails sent to mailing lists"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(m.message_ID)) as sent "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class EmailsSenders(Metrics):
    """ Emails Senders class for mailing list analysis """

    id = "senders"
    name = "Email Senders"
    desc = "People sending emails"
    data_source = MLS

    def __get_sql__ (self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables = tables + ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            #not sure if this line is useful anymore...
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       "mp.email_address = pup.people_id and "+\
                       "mp.type_of_recipient=\'From\' "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class SendersResponse(Metrics):
    """ People answering in a thread """
    # Threads class is not needed here. This is thanks to the use of the
    # field is_reponse_of.

    id = "senders_response"
    name = "Senders Response"
    desc = "People answering in a thread"
    data_source = MLS

    def __get_sql__ (self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders_response "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables += ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       "mp.email_address = pup.people_id and "+\
                       "mp.type_of_recipient=\'From\' "
        filters += " and m.is_response_of is not null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class SendersInit(Metrics):
    """ People initiating threads """

    id = "senders_init"
    name = "SendersInit"
    desc = "People initiating threads"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders_init "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables += ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       " mp.email_address = pup.people_id and "+\
                       " mp.type_of_recipient=\'From\' "
        filters += " and m.is_response_of is null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class EmailsSentResponse(Metrics):
    """ Emails sent as response """

    id = "sent_response"
    name = "SentResponse"
    desc = "Emails sent as response"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(m.message_ID)) as sent_response "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis) + " and m.is_response_of is not null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class EmailsSentInit(Metrics):
    """ Emails sent as initiating a thread """

    id = "sent_init"
    name = "EmailsSentInit"
    desc = "Emails sent to start a thread"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(m.message_ID)) as sent_init"
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis) + " and m.is_response_of is null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class Threads(Metrics):
    """ Number of threads """

    id = "threads"
    name = "Threads"
    desc = "Number of threads"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(m.is_response_of)) as threads"
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class Repositories(Metrics):
    """ Mailing lists repositories """

    id = "repositories"
    name = "Mailing Lists"
    desc = "Mailing lists with activity"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        #fields = " COUNT(DISTINCT(m."+rfield+")) AS repositories  "
        fields = " COUNT(DISTINCT(m.mailing_list_url)) AS repositories "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query

class Companies(Metrics):
    """ Companies participating in mailing lists """

    id = "companies"
    name = "Companies"
    desc = "Companies participating in mailing lists"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate, 
                                  self.filters.enddate, ['company', ''], evolutionary, 'companies')


class Domains(Metrics):
    """ Domains found in the analysis of mailing lists """

    id = "domains"
    name = "Domains"
    desc = "Domains found in the analysis of mailing lists """
    data_source = MLS

    def __get_sql__(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, ['domain', ''], evolutionary, 'domains')


class Countries(Metrics):
    """ Countries participating in mailing lists """

    id = "countries"
    name = "Countries"
    desc = "Countries participating in mailing lists """
    data_source = MLS

    def __get_sql__(self, evolutionary):
        return self.db.GetStudies(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, ['country', ''], evolutionary, 'countries')


class UnansweredPosts(Metrics):
    """ Unanswered posts in mailing lists """

    id = "unanswered_posts"
    name = "Unanswered Posts"
    desc = "Unanswered posts in mailing lists"""
    data_source = MLS

    def __get_date_from_month(self, monthid):
        # month format: year*12+month
        year = (monthid-1) / 12
        month = monthid - year*12
        day = 1
        current = str(year) + "-" + str(month) + "-" + str(day)
        return (current)

    def __get_messages(self, from_date, to_date):
        query = "SELECT message_ID, is_response_of "
        query += "FROM messages m "
        query += "WHERE m.first_date >= '" + str(from_date) + "' AND m.first_date < '" + str(to_date) + "' "
        query += "AND m.first_date >= " + str(self.filters.startdate) + " AND m.first_date < " + str(self.filters.enddate) + " "
        query += "ORDER BY m.first_date"

        results = ExecuteQuery(query)

        if isinstance(results['message_ID'], list):
            return [(results['message_ID'][i], results['is_response_of'][i])\
                    for i in range(len(results['message_ID']))]
        else:
            return [(results['message_ID'], results['is_response_of'])]

    def get_agg(self):
        return {}

    def get_ts(self):
        # Get all posts for each month and determine which from those
        # are still unanswered. Returns the number of unanswered
        # posts on each month.
        period = self.filters.period

        if (period != "month"):
            logging.error("Period not supported in " + self.id + " " + period)
            return None

        startdate = self.filters.startdate
        enddate = self.filters.enddate

        start = datetime.strptime(startdate, "'%Y-%m-%d'")
        end = datetime.strptime(enddate, "'%Y-%m-%d'")

        start_month = (start.year * 12 + start.month) - 1
        end_month = (end.year * 12 + end.month) - 1
        months = end_month - start_month + 2
        num_unanswered = {'month' : [],
                          'unanswered_posts' : []}

        for i in range(0, months):
            unanswered = []
            current_month = start_month + i
            from_date = self.__get_date_from_month(current_month)
            to_date = self.__get_date_from_month(current_month + 1)
            messages = self.__get_messages(from_date, to_date)

            for message in messages:
                message_id = message[0]
                response_of = message[1]

                if response_of is None:
                    unanswered.append(message_id)
                    continue

                if response_of in unanswered:
                    unanswered.remove(response_of)

            num_unanswered['month'].append(current_month)
            num_unanswered['unanswered_posts'].append(len(unanswered))

        return completePeriodIds(num_unanswered, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)
