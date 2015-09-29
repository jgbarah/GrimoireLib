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
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>

# SQL utilities

import MySQLdb
import logging
import re, sys
from vizgrimoire.metrics.query_builder import DSQuery


# global vars to be moved to specific classes
cursor = None
# one connection per database
dbpool = {}

##
## METAQUERIES
##

# TODO: regexpr not adapted yet from R to Python


def GetSQLGlobal(date, fields, tables, filters, start, end,
                 type_analysis = None):
    all_items = DSQuery.get_all_items(type_analysis)
    return DSQuery.GetSQLGlobal(date, fields, tables, filters, start, end, all_items)
    return(sql)

def GetSQLPeriod(period, date, fields, tables, filters, start, end,
                 type_analysis = None):
    all_items = DSQuery.get_all_items(type_analysis)
    return DSQuery.GetSQLPeriod(period, date, fields, tables, filters, start, end, all_items)

############
#Generic functions to check evolutionary or static info and for the execution of the final query
###########

def BuildQuery (period, startdate, enddate, date_field, fields, tables, filters, evolutionary):
    # Select the way to evolutionary or aggregated dataset
    q = ""

    if (evolutionary):
        q = GetSQLPeriod(period, date_field, fields, tables, filters,
                          startdate, enddate)
    else:
        q = GetSQLGlobal(date_field, fields, tables, filters,
                          startdate, enddate)

    return(q)

def SetDBChannel (user=None, password=None, database=None,
                  host="127.0.0.1", port=3306, group=None):
    global cursor
    global dbpool

    db = None

    if database in dbpool:
        db = dbpool[database]
    else:
        if (group == None):
            db = MySQLdb.connect(user=user, passwd=password,
                                 db=database, host=host, port=port)
        else:
            db = MySQLdb.connect(read_default_group=group, db=database)
        dbpool[database] = db

    cursor = db.cursor()
    cursor.execute("SET NAMES 'utf8'")

def ExecuteQuery (sql, enforce = None):
    """Execute query, return results.

    Returns the results of executing the specified SQL query, as a
    dictionary. In this dictionary, keys are field names for the query
    results, values are the corresponding value(s) for the corresponding
    field in all rows.
    
    Depending on the value of the parameter 'enforce', results will
    be enforced to be a list, or (default behavior, for compatibility),
    a list if results are more than one row, or single values if the
    result is only one row. If result is 0 rows, an empty list will always
    be returned for each key.

    For example, if the following values are returned by the query
    (assuming each line is a row, first line is the name of fields):

    "Field A", "Field B"
    "String 1", 1
    "String 2", 2

    the returning dictionary will be

    {"Field A": ["String 1", "String 2"],
     "Field B": [1, 2]
    }

    If query results is just one row, and enforce == "list", the result
    is as follows:

    {"Field A": ["String 1"],
     "Field B": [1]
    }

    In the same case, enforce == None, the result is:

    {"Field A": "String 1",
     "Field B": 1
    }

    If query results is 0 rows:

    {"Field A": [],
     "Field B": []
    }

    If for some reason the the description of the fields cannot be obtained,
    it returns None (if enforce =) "list") or empty directory
    (if enforce == None). Probably it should raise an exception.

    :param sql: SQL string to execute
    :param enforce: enforce some kind of result, "list" or None (default)

    :returns result of executing the SQL string

    """

    cursor.execute(sql)
    rows = cursor.rowcount
    columns = cursor.description

    if columns is None:
        if enforce == "list":
            return None
        else:
            return {}

    result = {}

    for column in columns:
        result[column[0]] = []
    if rows > 1 or ((rows == 1) and (enforce == "list")):
        for value in cursor.fetchall():
            for (index,column) in enumerate(value):
                result[columns[index][0]].append(column)
    elif rows == 1:
        value = cursor.fetchone()
        for i in range (0, len(columns)):
            result[columns[i][0]] = value[i]
    return result 
