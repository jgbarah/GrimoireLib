#!/usr/bin/env python

# Copyright (C) 2012, 2013 Bitergia
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
# Authors:
#   Alvaro del Castillo <acs@bitergia.com>
#

import logging, sys, time

from GrimoireUtils import read_options, getPeriod, read_main_conf
from report import Report

def get_evol_report(startdate, enddate, identities_db, bots):
    all_ds = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        all_ds[ds.get_name()] = ds.get_evolutionary_data (period, startdate, enddate, identities_db)
    return all_ds

def create_evol_report(startdate, enddate, destdir, identities_db, bots):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        ds.create_evolutionary_report (period, startdate, enddate, destdir, identities_db)

def get_agg_report(startdate, enddate, identities_db, bots):
    all_ds = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        all_ds[ds.get_name()] = ds.get_agg_data (period, startdate, enddate, identities_db)
    return all_ds

def create_agg_report(startdate, enddate, destdir, identities_db, bots):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        ds.create_agg_report (period, startdate, enddate, destdir, identities_db)

def get_top_report(startdate, enddate, identities_db, bots):
    all_ds_top = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        top = ds.get_top_data (startdate, enddate, identities_db, opts.npeople)
        all_ds_top[ds.get_name()] = top 
    return all_ds_top

def create_top_report(startdate, enddate, destdir, npeople, identities_db, bots):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        ds.create_top_report (startdate, enddate, destdir, npeople, identities_db)

def create_reports_filters(period, startdate, enddate, destdir, npeople, identities_db, bots):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        logging.info("Creating filter reports for " + ds.get_name())
        for filter_ in Report.get_filters():
            logging.info("-> " + filter_.get_name())
            ds.create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots)

def create_report_people(startdate, enddate, destdir, npeople, identities_db, bots):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        logging.info("Creating people for " + ds.get_name())
        ds().create_people_report(period, startdate, enddate, destdir, npeople, identities_db)

def create_reports_r(enddate, destdir):
    from rpy2.robjects.packages import importr
    opts = read_options()

    vizr = importr("vizgrimoire")

    for ds in Report.get_data_sources():
        automator = Report.get_config()
        db = automator['generic'][ds.get_db_name()]
        vizr.SetDBChannel (database=db, user=opts.dbuser, password=opts.dbpassword)
        logging.info("Creating R reports for " + ds.get_name())
        ds.create_r_reports(vizr, enddate, destdir)

def set_data_source(ds_name):
    ds_ok = False
    dss_active = Report.get_data_sources()
    for ds in dss_active:
        if ds.get_name() == opts.data_source:
            ds_ok = True
            Report.set_data_sources([ds])
    if not ds_ok:
        logging.error(opts.data_source + " data source not available")
        sys.exit(1)

def set_filter(filter_name):
    filter_ok = False
    filters_active = Report.get_filters()
    for filter_ in filters_active:
        if filter_.get_name() == opts.filter:
            filter_ok = True
            Report.set_filters([filter_])
    if not filter_ok:
        logging.error(opts.filter + " filter not available")
        sys.exit(1)

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting Report analysis")
    opts = read_options()
    reports = opts.reports.split(",")

    Report.init(opts.config_file)

    automator = read_main_conf(opts.config_file)
    if 'start_date' not in automator['r']:
        logging.error("start_date (yyyy-mm-dd) not found in " + opts.config_file)
        sys.exit()
    start_date = automator['r']['start_date']
    if 'end_date' not in automator['r']:
        end_date = time.strftime('%Y-%m-%d')
    else:
        end_date = automator['r']['end_date']

    if 'period' not in automator['r']:
        period = getPeriod("months")
    else:

        period = getPeriod(automator['r']['period'])
    logging.info("Period: " + period)

    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+start_date+"'"
    enddate = "'"+end_date+"'"

    identities_db = automator['generic']['db_identities']

    if (opts.data_source):
        set_data_source(opts.data_source)
    if (opts.filter):
        set_filter(opts.filter)

    bots = []

    logging.info("Creating global evolution metrics...")
    evol = create_evol_report(startdate, enddate, opts.destdir, identities_db, bots)
    logging.info("Creating global aggregated metrics...")
    agg = create_agg_report(startdate, enddate, opts.destdir, identities_db, bots)
    logging.info("Creating global top metrics...")
    top = create_top_report(startdate, enddate, opts.destdir, opts.npeople, identities_db, bots)

    create_reports_filters(period, startdate, enddate, opts.destdir, opts.npeople, identities_db, bots)
    if (automator['r']['reports'].find('people')>-1):
        create_report_people(startdate, enddate, opts.destdir, opts.npeople, identities_db, bots)
    create_reports_r(end_date, opts.destdir)

    logging.info("Report data source analysis OK")