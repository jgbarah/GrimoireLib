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
# This file is a part of the vizGrimoire.R package
#
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Alvaro del Castillo San Felix <acs@bitergia.com>
##   Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#
# Usage:
#     PYTHONPATH=../vizgrimoire LANG= R_LIBS=../../r-lib ./mls-analysis.py 
#                                                -d acs_irc_automatortest_2388_2 -u root 
#                                                -i acs_cvsanaly_automatortest_2388 
#                                                -s 2010-01-01 -e 2014-01-20 
#                                                -o ../../../json -r people,repositories
#

import logging
import sys

from rpy2.robjects.packages import importr
isoweek = importr("ISOweek")
vizr = importr("vizgrimoire")

import GrimoireUtils, GrimoireSQL
from GrimoireUtils import dataFrame2Dict, createJSON, completePeriodIds
from GrimoireUtils import valRtoPython, read_options, getPeriod
import ITS
from ITS import Backend

def aggData(period, startdate, enddate, identities_db, destdir, closed_condition):
    data = ITS.AggITSInfo(period, startdate, enddate, identities_db, [], closed_condition)
    agg = data
    data = ITS.AggAllParticipants(startdate, enddate)
    agg = dict(agg.items() +  data.items())
    data = ITS.TrackerURL()
    agg = dict(agg.items() +  data.items())

    if ('companies' in reports):
        data = ITS.AggIssuesCompanies(period, startdate, enddate, identities_db)
        agg = dict(agg.items() + data.items())

    if ('countries' in reports):
        data = ITS.AggIssuesCountries(period, startdate, enddate, identities_db)
        agg = dict(agg.items() + data.items())

    if ('domains' in reports):
        data = ITS.AggIssuesDomains(period, startdate, enddate, identities_db)
        agg = dict(agg.items() + data.items())

    # Tendencies    
    for i in [7,30,365]:
        period_data = ITS.GetDiffClosedDays(period, identities_db, enddate, i, [], closed_condition)
        agg = dict(agg.items() + period_data.items())
        period_data = ITS.GetDiffOpenedDays(period, identities_db, enddate, i, [])
        agg = dict(agg.items() + period_data.items())
        period_data = ITS.GetDiffClosersDays(period, identities_db, enddate, i, [], closed_condition)
        agg = dict(agg.items() + period_data.items())
        period_data = ITS.GetDiffChangersDays(period, identities_db, enddate, i, [])
        agg = dict(agg.items() + period_data.items())

    # Last Activity: to be removed
    for i in [7,14,30,60,90,180,365,730]:
        period_activity = ITS.GetLastActivityITS(i, closed_condition)
        agg = dict(agg.items() + period_activity.items())

    createJSON (agg, destdir+"/its-static.json")

def tsData(period, startdate, enddate, identities_db, destdir, granularity,
           conf, backend):

    closed_condition = backend.closed_condition
    data = ITS.EvolITSInfo(period, startdate, enddate, identities_db, [], closed_condition)
    evol = completePeriodIds(data, period, startdate, enddate)
    if ('companies' in reports) :
        data = ITS.EvolIssuesCompanies(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

    if ('countries' in reports) :
        data = ITS.EvolIssuesCountries(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

    if ('repositories' in reports) :
        data = ITS.EvolIssuesRepositories(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

    if ('domains' in reports) :
        data = ITS.EvolIssuesDomains(period, startdate, enddate, identities_db)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

    data = ticketsStates(period, startdate, enddate, identities_db, backend)
    evol = dict(evol.items() + data.items())

    createJSON (evol, destdir+"/its-evolutionary.json")

def peopleData(period, startdate, enddate, identities_db, destdir, closed_condition, top_data):
    top = top_data['closers.']["id"]
    top += top_data['closers.last year']["id"]
    top += top_data['closers.last month']["id"]
    top += top_data['openers.']["id"]
    top += top_data['openers.last year']["id"]
    top += top_data['openers.last month']["id"]
    # remove duplicates
    people = list(set(top))
    # the order is not the same than in R json
    createJSON(people, destdir+"/its-people.json")

    for upeople_id in people :
        evol = ITS.GetPeopleEvolITS(upeople_id, period, startdate, enddate, closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        createJSON (evol, destdir+"/people-"+str(upeople_id)+"-its-evolutionary.json")

        data = ITS.GetPeopleStaticITS(upeople_id, startdate, enddate, closed_condition)
        createJSON (data, destdir+"/people-"+str(upeople_id)+"-its-static.json")

def reposData(period, startdate, enddate, identities_db, destdir, conf, closed_condition):
    repos  = ITS.GetReposNameITS(startdate, enddate)
    repos = repos['name']
    if not isinstance(repos, (list)): 
        repos = [repos]
    createJSON(repos, destdir+"/its-repos.json")

    for repo in repos :
        repo_name = "'"+ repo+ "'"
        repo_file = repo.replace("/","_")
        evol = ITS.EvolITSInfo(period, startdate, enddate, identities_db, ['repository', repo_name], closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        createJSON(evol, destdir+"/"+repo_file+"-its-rep-evolutionary.json")

        agg = ITS.AggITSInfo(period, startdate, enddate, identities_db, ['repository', repo_name], closed_condition)

        createJSON(agg, destdir+"/"+repo_file+"-its-rep-static.json")

def companiesData(period, startdate, enddate, identities_db, destdir, closed_condition, bots, npeople):
    companies  = ITS.GetCompaniesNameITS(startdate, enddate, identities_db, closed_condition, bots)
    companies = companies['name']
    createJSON(companies, destdir+"/its-companies.json")

    for company in companies:
        company_name = "'"+ company+ "'"
        print (company_name)

        evol = ITS.EvolITSInfo(period, startdate, enddate, identities_db, ['company', company_name], closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        createJSON(evol, destdir+"/"+company+"-its-com-evolutionary.json")

        agg = ITS.AggITSInfo(period, startdate, enddate, identities_db, ['company', company_name], closed_condition)
        createJSON(agg, destdir+"/"+company+"-its-com-static.json")

        top = ITS.GetCompanyTopClosers(company_name, startdate, enddate, identities_db, bots, closed_condition, npeople)
        createJSON(top, destdir+"/"+company+"-its-com-top-closers.json", False)

    closed = ITS.GetClosedSummaryCompanies(period, startdate, enddate, identities_db, closed_condition, 10)
    createJSON (closed, opts.destdir+"/its-closed-companies-summary.json")

def countriesData(period, startdate, enddate, identities_db, destdir, closed_condition):
    countries  = ITS.GetCountriesNamesITS(startdate, enddate, identities_db, closed_condition)
    countries = countries['name']
    createJSON(countries, destdir+"/its-countries.json")

    for country in countries :
        print (country)

        country_name = "'" + country + "'"
        evol = ITS.EvolITSInfo(period, startdate, enddate, identities_db, ['country', country_name], closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        createJSON (evol, destdir+"/"+country+"-its-cou-evolutionary.json")

        data = ITS.AggITSInfo(period, startdate, enddate, identities_db, ['country', country_name], closed_condition)
        createJSON (data, destdir+"/"+country+"-its-cou-static.json")

def domainsData(period, startdate, enddate, identities_db, destdir, closed_condition, bots, npeople):
    domains = ITS.GetDomainsNameITS(startdate, enddate, identities_db, closed_condition, bots)
    domains = domains['name']
    createJSON(domains, destdir+"/its-domains.json")

    for domain in domains:
        domain_name = "'"+ domain + "'"
        print (domain_name)

        evol = ITS.EvolITSInfo(period, startdate, enddate, identities_db, ['domain', domain_name], closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        createJSON(evol, destdir+"/"+domain+"-its-dom-evolutionary.json")

        agg = ITS.AggITSInfo(period, startdate, enddate, identities_db, ['domain', domain_name], closed_condition)
        createJSON(agg, destdir+"/"+domain+"-its-dom-static.json")

        top = ITS.GetDomainTopClosers(domain_name, startdate, enddate, identities_db, bots, closed_condition, npeople)
        createJSON(top, destdir+"/"+domain+"-its-dom-top-closers.json")

def topData(period, startdate, enddate, identities_db, destdir, bots, closed_condition, npeople):
    # Top closers
    top_closers_data = {}
    # top_closers_data['closers.']=dataFrame2Dict(vizr.GetTopClosers(0, startdate, enddate,identities_db, bots, closed_condition))
    top_closers_data['closers.']=ITS.GetTopClosers(0, startdate, enddate,identities_db, None, closed_condition, npeople)
    top_closers_data['closers.last year']=ITS.GetTopClosers(365, startdate, enddate,identities_db, None, closed_condition, npeople)
    top_closers_data['closers.last month']=ITS.GetTopClosers(31, startdate, enddate,identities_db, None, closed_condition, npeople)

    # Top openers
    top_openers_data = {}
    top_openers_data['openers.']=ITS.GetTopOpeners(0, startdate, enddate,identities_db, None, closed_condition, npeople)
    top_openers_data['openers.last year']=ITS.GetTopOpeners(365, startdate, enddate,identities_db, None, closed_condition, npeople)
    top_openers_data['openers.last month']=ITS.GetTopOpeners(31, startdate, enddate,identities_db, None, closed_condition, npeople)


    all_top = dict(top_closers_data.items() + top_openers_data.items())
    createJSON (all_top, destdir+"/its-top.json")

    return all_top

def microStudies(vizr, destdir):
    # Studies implemented in R

    # Time to Close: Other backends not yet supported
    vizr.ReportTimeToCloseITS(opts.backend, opts.destdir)

    unique_ids = True
    # Demographics
    vizr.ReportDemographicsAgingITS(opts.enddate, opts.destdir, unique_ids)
    vizr.ReportDemographicsBirthITS(opts.enddate, opts.destdir, unique_ids)

    # Markov
    # vizr.ReportMarkovChain(opts.destdir)

def ticketsStates(period, startdate, enddate, identities_db, backend):
    evol = {}
    return evol
    for status in backend.statuses:
        print ("Working with ticket status: " + status)
        #Evolution of the backlog
        tickets_status = vizr.GetEvolBacklogTickets(period, startdate, enddate, status, backend.name_log_table)
        tickets_status = dataFrame2Dict(tickets_status)
        tickets_status = completePeriodIds(tickets_status, period, startdate, enddate)
        # rename key
        tickets_status[status] = tickets_status.pop("pending_tickets")
        #Issues per status
        current_status = vizr.GetCurrentStatus(period, startdate, enddate, identities_db, status)
        current_status = completePeriodIds(dataFrame2Dict(current_status), period, startdate, enddate)
        #Merging data
        evol = dict(evol.items() + current_status.items() + tickets_status.items())
    return evol

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting ITS data source analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")

    # filtered bots
    bots = ['-Bot']

    # Working at the same time with VizR and VizPy yet
    vizr.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)
    GrimoireSQL.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)

    (startdate, enddate) = ITS.get_timespan()
    # TODO: hack because VizR library needs. Fix in lib in future
    startdate_str = startdate.strftime('%Y-%m-%d')
    enddate_str = enddate.strftime('%Y-%m-%d')
    startdate = "'"+startdate_str+"'"
    enddate = "'"+enddate_str+"'"

    # backends
    backend = Backend(opts.backend)

    tsData (period, startdate, enddate, opts.identities_db, opts.destdir, 
            opts.granularity, opts, backend)
    aggData(period, startdate, enddate, opts.identities_db, opts.destdir, backend.closed_condition)

    top = topData(period, startdate, enddate, opts.identities_db, opts.destdir, bots, backend.closed_condition, opts.npeople)

    microStudies(vizr, opts.destdir)

    if ('people' in reports):
        peopleData (period, startdate, enddate, opts.identities_db, opts.destdir, backend.closed_condition, top)
    if ('repositories' in reports):
        reposData (period, startdate, enddate, opts.identities_db, opts.destdir, opts, backend.closed_condition)
    if ('countries' in reports):
        countriesData (period, startdate, enddate, opts.identities_db, opts.destdir, backend.closed_condition)
    if ('companies' in reports):
        companiesData (period, startdate, enddate, opts.identities_db, opts.destdir, backend.closed_condition, bots, opts.npeople)
    if ('domains' in reports):
        domainsData (period, startdate, enddate, opts.identities_db, opts.destdir, backend.closed_condition, bots, opts.npeople)

    logging.info("ITS data source analysis OK!")