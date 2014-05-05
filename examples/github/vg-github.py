#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (C) 2013, 2014 Jesus M. Gonzalez-Barahona
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors :
#       Jesus M. Gonzalez-Barahona <jgb@bitergia.com>

#
# vg-github.py
#
# Simple script to retrieve data from GitHub repositories about a project,
# or all the projects owned by a user.
# It installs CVSAnalY and Bicho (form MetricsGrimoire git repositories),
# the needed packages from vizGrimoire git repositories, R dependencies,
# and main Python dependencies.
#
# There are options for not installing everything once again if the script
# is run again, run it with --help to get a full list.
#
# Example of how to produce a dashboard for repository
# VizGrimoire/VizGrimoireR:
#
# vg-github.py --user jgb --passwd XXX --dir /tmp/vgr --removedb
#  --ghuser ghuser --ghpasswd XXX VizGrimoire/VizGrimoireR
#
# Example of how to produce a dashboard for all repositories owned by
# organization MetricsGrimoire:
#
# vg-github.py --user jgb --passwd XXX --dir /tmp/vgr --removedb
#  --ghuser ghuser --ghpasswd XXX --isuser MetricsGrimoire

import argparse
import MySQLdb
from contextlib import closing
import os
import shutil
import errno
from subprocess import call, Popen, PIPE
import urllib2
import json
import string

mgConf = {}
rConf = {}
args = None
dbPrefix = ""
dir = ""
dashboard_dir = ""
JSONdir = ""


def _prepare_db (tool, name, user, passwd, remove = True):
    """Prepare MetricsGrimoire database.

    Prepares (and deletes, if args.removedb was specified) the database
    for a MetricsGrimoire tool.
    This is usually run once per tool, just before the calls to run the tools

    Parameters
    ----------

    tool: {'cvsanaly', 'bicho'}
        Tool for which to prepare database.
    name: string
        Name of the database to prepare.
    user: string
        User name to access the database.
    passwd: string
        Password to access the MySQL database.
    db_remove: Boolean
        Whether to remove the database before preparing it
        (default: True).

    Returns
    -------

    None

    """

    # Open database connection and get a cursor
    conn = MySQLdb.connect(host='localhost', user=user, passwd=passwd)
    # with clause ensures that connection is closed (and committed) even
    # in the case of exceptions
    with closing(conn.cursor()) as cursor:
        # Create database and remove it in advance, if needed
        if remove:
            cursor.execute('DROP DATABASE IF EXISTS ' + name)
        cursor.execute('CREATE DATABASE IF NOT EXISTS ' + name +
                       ' CHARACTER SET utf8 COLLATE utf8_unicode_ci')
    conn.close()


def find_repos (user):
    """Find the repos for a GitHub user or organization.

    Gets a list of the repositories (projects) owned by a GitHub
    user or organization, by querying the GitHub REST API.

    Parameters
    ----------

    user: string
        GitHub user or organziation

    Returns
    -------

    tuple of strings
        Repository names.

    """

    repos_url = 'https://api.github.com/users/' + user + '/repos'
    res = urllib2.urlopen(repos_url)
    repos_json = res.read()
    repos = json.loads(repos_json)
    repo_names = [repo['full_name'] for repo in repos]
    return (repo_names)


def run_mgtool (tool, project, dbname):
    """Run MetricsGrimoire tool

    - tool: cvsanaly | bicho
    - project: GitHub project, such as VizGrimoire/VizGrimoireR
    - dbname: name of the database

    Uses information in global dictionary conf for deciding
    about options for the tool.
    """

    # Prepare options to run the tool
    tool_bin = os.path.join(mgConf["dir"],
                            mgConf[tool]["dir"],
                            mgConf[tool]["bin"])
    opts = [tool_bin]
    opts.extend (mgConf[tool]["opts"])
    if args.user:
        opts.extend ([mgConf[tool]["dbuser"], args.user])
    if args.passwd:
        opts.extend ([mgConf[tool]["dbpasswd"], args.passwd])
    opts.extend ([mgConf[tool]["db"], dbname])
    # Specific code for running cvsanaly
    if tool == "cvsanaly":
        gitdir = project.split('/', 1)[1]
        call(["git", "clone", "https://github.com/" + project + ".git",
              dir + '/repos/' + gitdir])
        opts.append ("--extensions=" + "CommitsLOC")
        opts.append (dir + '/repos/' + gitdir)
        if not args.verbose:
             opts.append ("--quiet")
    # Specific code for running bicho
    if tool == "bicho":
        opts.extend (["--url",
                     "https://api.github.com/repos/" + project + "/issues",
                      "--backend-user", args.ghuser,
                      "--backend-password", args.ghpasswd])
    print "Running MetricsGrimoire tool (" + tool + ")" 
    # Run the tool
    env = os.environ.copy()
    env ["PYTHONPATH"] = mgConf[tool]["ppath"] + ":" + \
        os.environ.get("PYTHONPATH", "")
    if args.verbose:
        print "PYTHONPATH: " + env ["PYTHONPATH"]
        print "Running: " + " ".join(opts)
    call(opts, env=env)


def run_mgtools (tools, projects, dbprefix):
    """Run MetricsGrimoire tools

    - tools: [cvsanaly, bicho, ...] (list)
    - project: GitHub project, such as VizGrimoire/VizGrimoireR
    - dbprefix: prefix for the name of the database

    Run the specified MetricsGRimoire tools, preparing their
    corresponding databases if needed

    """

    for tool in tools:
        # Prepare databases
        dbname = dbprefix + "_" + tool
        _prepare_db (tool, dbname, args.user, args.passwd, args.removedb)
        # Run tools
        for project in projects:
            run_mgtool (tool, project, dbname)

def create_rlib (libdir):
    """Create directory for the R library

    -  libdir: directory to install R libraries
    """

    try:
        os.makedirs(libdir)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(libdir):
            pass
        else: 
            raise

def install_from_git (dir, pkgs, conf):
    """Install some source packages from their git repos.

    Just clone the repos if corresponding subdirectories don't exist,
    or pull from them if they exist.
    For each pkg, a corresponding entry in conf must exist. That entry
    should be a dictionary, with at lease two entries:
    - "dir": the subdirectory name for installation
    - "repo": the url of the git repository

    Parameters
    ----------
    dir : string
        Path of directory to install vizGrimoire packages as subdirectories
    pkgs: sequence of strings
        List of packages to install
    conf: dictionay
        Keys are pkg names, values are dictionaries (see description above)

    Returns
    -------
    None

    """

    if not os.path.exists(dir):
        os.makedirs(dir)
    for pkg in pkgs:
        dir_pkg = os.path.join(dir, conf[pkg]["dir"])
        if not os.path.exists(dir_pkg):
            call(["git", "clone", conf[pkg]["repo"], dir_pkg])
        else:
            call(["git", "--git-dir=" + dir_pkg + "/.git", "pull"])

def install_vizgrimoirer (libdir, vizgrimoirer_pkgdir):
    """Install the appropriate vizgrimore R package in a specific location

    - libdir: directory to install R libraries
    - vizgrimoirer_pkgdir: directory with the source code for the
        VizGrimoireR R package

    Installing the package to ensure it is properly installed,
    even if it is not available from the standard R librdirs,
    or the version there is not the right one.
    Installs R dependencias, only if libdir is created.

    """

    env = os.environ.copy()
    env ["R_LIBS"] = libdir
    call (["R", "CMD", "INSTALL", vizgrimoirer_pkgdir], env=env)

def install_rdepend (libdir, vizgrimoirer_pkgdir):
    """Install R dependencies in a specific location

    - libdir: directory to install R libraries
    - vizgrimoirer_pkgdir: directory with the source code for the
        VizGrimoireR R package

    Installing R dependencies, obtained by reading the DESCRIPTION
    file in the source package for VizGrimoire R.

    """

    # Extract dependant R packages from Depends line in DESCRIPTION
    descFile = open(vizgrimoirer_pkgdir + "/DESCRIPTION")
    lines = descFile.readlines()
    for line in lines:
        (field, content) = line.split(":")
        if field == "Depends":
            pkgList = content.split(",")
            break
    # Build R vector with packages to install
    pkgVector = 'c('
    first = True
    for pkg in pkgList:
        if first:
            first = False
        else:
            pkgVector = pkgVector + ','
        pkgVector = pkgVector + '"' + pkg.strip() + '"'
    pkgVector = pkgVector + ')'
    # Run R to install all packages
    # rcode = 'install.packages(' + pkgVector + ', lib="' + \
    #        libdir + '", repos="http://cran.rstudio.com/", ' + \
    #        'dependencies=c("Depends"))\n'
    rcode = 'install.packages(' + pkgVector + ', lib="' + \
           libdir + '", repos="http://cran.rstudio.com/")\n'
    env = os.environ.copy()
    env ["R_LIBS"] = libdir
    p = Popen(["R", "--vanilla"], stdin=PIPE)
    p.communicate(rcode)

def install_pdepend (libdir, libs):
    """Install R dependencies in a specific location

    - libdir: directory to install Python libraries

    Installing Python dependencies, using pip

    """

    for lib in libs:
        call (["pip", "install", "--target=" + libdir, lib])

def unique_ids (dbprefix):
    """Run unique identities stuff

    - dbprefix: prefix for the databases

    """

    print os.path.join(vgConf["dir"],
                        vgConf["vizGrimoireUtils"]["dir"],
                        vguConf["unifypeople"])
    call ([os.path.join(vgConf["dir"],
                        vgConf["vizGrimoireUtils"]["dir"],
                        vguConf["unifypeople"]),
           "-d", dbprefix + "_" + "cvsanaly",
           "-u", args.user, "-p", args.passwd, "-i", "no"])
    call ([os.path.join(vgConf["dir"],
                        vgConf["vizGrimoireUtils"]["dir"],
                        vguConf["ds2id"]),
           "--data-source=its",
           "--db-name-ds=" + dbprefix + "_" + "bicho",
           "--db-name-ids=" + dbprefix + "_" + "cvsanaly",
           "-u", args.user, "-p", args.passwd])

def affiliation (dbprefix):
    """Run affiliation stuff

    - dbprefix: prefix for the databases

    """

    call ([os.path.join(vgConf["dir"],
                        vgConf["vizGrimoireUtils"]["dir"],
                        vguConf["domains"]),
           "-d", dbprefix + "_" + "cvsanaly",
           "-u", args.user, "-p", args.passwd])



def run_analysis (scripts, base_dbs, id_dbs, outdir):
    """Run analysis scripts

    - scripts: scripts to run (list)
    - base_dbs: base database for each script (list)
    - id_dbs: identities database for each script (list)
    - outdir: directory to write output (JSON) files

    The vizgrimoirer R package has to be installed in the R path
    (run install_vizgrimoirer in case of doubt)

    """

    # Create the JSON data directory for the scripts to write to
    try:
        os.makedirs(outdir)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(outdir):
            pass
        else: 
            raise
    # Run the analysis scripts
    env = os.environ.copy()
    env["R_LIBS"] = rConf["libdir"] + ":" + os.environ.get("R_LIBS", "")
    env["PYTHONPATH"] = glConf["libdir"] + ":" + \
        pythonConf["libdir"] + ":" + \
        os.environ.get("PYTHONPATH", "")
    env["LANG"] = ""
    for script, base_db, id_db in zip (scripts, base_dbs, id_dbs):
        call_list = [script, "-d", base_db,
                     "--dbuser", args.user, "--dbpassword", args.passwd,
                     "-i", id_db,
                     "--granularity", "weeks",
                     "--destination", outdir]
        if args.verbose:
            print "Running: " + " ".join (call_list)
        call (call_list, env=env)

def produce_config (config_template, config_file):
    """Produce a config.json file by translating a template file.

    - config_template: template for config.json (filename)
    - config_file: config.json file to be produced (filename)

    """

    if args.verbose:
        print ("Producing config file " + config_file + \
                   " from template " + config_template + ".")
    subst = dict (start_date = "2002-10-10",
                  end_date = "2013-12-31",
                  project_name = args.name,
                  project_url = "http://github.com/" + args.name,
                  scm_url = "",
                  its_url = "")
    template_str = ""
    with open (config_template, "r") as template:
        template_str = template.read()
    template = string.Template(template_str)
    config_str = template.safe_substitute (subst)
    with open(config_file, "w") as config:
        config.write(config_str)
        config.close()

def produce_dashboard (vizgrimoirejs_dir, example_dir,
                       dashboard_dir, json_dir):
    """Produce an HTML dashboard for the JSON files

    - vizgrimoirejs_dir: dir with the source for VizGrimoireJS 
    - example_dir: dir with the source specific for the example 
    - dashboard_dir: dir to copy dashboard files to
    - json_dir: dir for json files

    Produce an HTML dashboard for the JSON files generated by the
    analysis scripts.

    """

    # Files from vizGrimoireJS to copy:
    vgjsFiles = ["vizgrimoire.min.js",
                 "lib/jquery-1.7.1.min.js",
                 "bootstrap/js/bootstrap.min.js",
                 "vizgrimoire.css",
                 "browser/custom.css",
                 "bootstrap/css/bootstrap.min.css",
                 "bootstrap/css/bootstrap-responsive.min.css",
                 "browser/favicon.ico"]
    # Files specific to this GitHub example:
    ghBrowserfiles = ["index.html",
                      "navbar.html", "footer.html", "refcard.html",
                      "project-card.html",
                      "viz_cfg.json", "custom.css"]

    for file in vgjsFiles:
        shutil.copy(vizgrimoirejs_dir + "/" + file, dashboard_dir)
    for file in ghBrowserfiles:
        shutil.copy(example_dir + "/" + file, dashboard_dir)
    produce_config (example_dir + "/config.json",
                    dashboard_dir + "/config.json")

if __name__ == "__main__":

    def parse_args ():
        """Parse command line arguments"""

        parser = argparse.ArgumentParser(description="""
Simple script to retrieve data from GitHub repositories about a project.
It creates MySQL databases named projectname_cvsanaly, projectname_bicho
(assummes permission to create databases), but
refrains to do so if they already exist (projectname will have
/ changed to _).
It assumes MetricsGrimoire tools are already installed.
If you don't know how to install them, look at
misc/metricsgrimoire-setup.py""")
        parser.add_argument("name",
                            help="GitHub project or user (if --isuser) name")
        parser.add_argument("--isuser",
                            help="Name is the user who owns projects to analyze",
                            action="store_true")
        parser.add_argument("--dbprefix",
                            help="Prefix for MySQL database (default: name argument)")
        parser.add_argument("--user",
                            help="MySQL user name")
        parser.add_argument("--passwd",
                            help="MySQL password")
        parser.add_argument("--dir",
                            help="Extraction directory (must exist). Default: /tmp")
        parser.add_argument("--removedb",
                            help="Remove all databases, if present, before creating them",
                            action="store_true")
        parser.add_argument("--nomg",
                            help="Don't run MetricsGrimoire tools",
                            action="store_true")
        parser.add_argument("--nopeople",
                            help="Don't run people stuff (unique ids, affiliation)",
                            action="store_true")
        parser.add_argument("--noinstvg",
                            help="Don't install vizGrimoire packages from repos",
                            action="store_true")
        parser.add_argument("--noinstvgr",
                            help="Don't install vizgrimoire R package",
                            action="store_true")
        parser.add_argument("--nordep",
                            help="Don't install R dependencies",
                            action="store_true")
        parser.add_argument("--nopythondep",
                            help="Don't install Python dependencies",
                            action="store_true")
        parser.add_argument("--noanalysis",
                            help="Don't run vizGrimoireR analysis",
                            action="store_true")
        parser.add_argument("--nobrowser",
                            help="Don't copy files for the browser",
                            action="store_true")
        parser.add_argument("--ghuser",
                            help="GitHub user name")
        parser.add_argument("--ghpasswd",
                            help="GitHub password")
        parser.add_argument("--vgdir",
                            help="Directory with vigGrimoireR, vizGrimoireJS and vizGrimoireUtils directories")
        parser.add_argument("--verbose",
                            help="Print out some messages about what's happening",
                            action="store_true")
        
        args = parser.parse_args()
        return (args)

    args = parse_args()
    if args.dbprefix:
        dbPrefix = args.dbprefix.lower()
    elif not args.isuser:
        dbPrefix = args.name.replace('/', '_').replace('-','_').lower()
    else:
        dbPrefix = args.name.replace('-','_').lower()
    if args.dir:
        dir = args.dir
    else:
        dir = "/tmp"

    # Directory for this script
    my_dir = os.path.dirname(os.path.realpath(__file__))
    # GrimoireLib directory is two levels up
    gl_dir = os.path.split(os.path.split(my_dir)[0])[0]
    # Root directory for the dashboard
    dashboard_dir = dir + "/dashboard"
    # JSON directory for browser
    JSONdir = dashboard_dir + "/data/json"

    # Configuration for MetricsGrimoire
    mgConf = {}

    # Configure R paths
    rConf = {"libdir": dir + "/rlib",
             "vgrpkg": os.path.join(gl_dir, "vizgrimoire"),
             }
    # Configure Python (paths, dependencies)
    pythonConf = {"libdir": dir + "/pythonlib",
                  "libs": ["rpy2"]}
    # Configure GrimoireLib paths
    glConf = {"libdir": os.path.join(gl_dir, "vizgrimoire"),
              "scm-analysis": os.path.join(my_dir, "scm-analysis.py"),
              "its-analysis": os.path.join(my_dir, "its-analysis.py"),
              }
    # Configuration for other vizGrimoire packages
    vgConf = {"pkgs": ["vizGrimoireUtils", "vizGrimoireJS"],
              "dir": os.path.join(args.dir, "vizGrimoire")
              }
    vgConf["vizGrimoireUtils"] = {
        "dir": "vizGrimoireUtils",
        "repo": "https://github.com/VizGrimoire/VizGrimoireUtils.git"
        }
    vgConf["vizGrimoireJS"] = {
        "dir": "vizGrimoireJS",
        "repo": "https://github.com/VizGrimoire/VizGrimoireJS.git"
        }
            
    # Configure vizGrimoireUtils paths
    vguConf = {"unifypeople": os.path.join("identities", "unifypeople.py"),
               "ds2id": os.path.join("identities", "datasource2identities.py"),
               "domains": os.path.join("identities", "domains_analysis.py")
               }
    # Now, if there is no --nomg flag, run MetricsGrimoire tools
    # If it is for a github user, get all the projects under the user name,
    # and run tools on each of them.
    # If it is for a single project, just run the tools on it
    if not args.nomg:
        # Location of MetricsGrimoire repositories
        mgConf["repo"] = "https://github.com/MetricsGrimoire/"
        mgConf["dir"] = os.path.join(args.dir, "mg")
        mgConf["tools"] = ["cvsanaly", "repohandler", "bicho"]
        mgConf["bintools"] = ["cvsanaly", "bicho"]
        mgConf["repohandler"] = {
            "repo": mgConf["repo"] + "RepositoryHandler",
            "dir": os.path.join(mgConf["dir"], "RepositoryHandler")
            }
        mgConf["cvsanaly"] = {
            "repo": mgConf["repo"] + "CVSAnalY",
            "dir": "CVSAnalY",
            "bin": os.path.join(mgConf["dir"], "CVSAnalY", "cvsanaly2"),
            "opts": [],
            "dbuser": "--db-user",
            "dbpasswd": "--db-password",
            "db": "--db-database"
            }
        mgConf["cvsanaly"]["ppath"] = mgConf["repohandler"]["dir"] + \
            ":" + os.path.join(mgConf["dir"], mgConf["cvsanaly"]["dir"])
        mgConf["bicho"] = {
            "repo": mgConf["repo"] + "Bicho",
            "dir": "Bicho",
            "bin": os.path.join("bin", "bicho"),
            "opts": ["-d", "1", "-b", "github"],
            "dbuser": "--db-user-out",
            "dbpasswd": "--db-password-out",
            "db": "--db-database-out"
            }
        mgConf["bicho"]["ppath"] = os.path.join(mgConf["dir"],
                                                mgConf["bicho"]["dir"])
        install_from_git (mgConf["dir"], mgConf["tools"], mgConf)
        if args.isuser:
            repos = find_repos (args.name)
        else:
            repos = [args.name]
        run_mgtools (["cvsanaly", "bicho"], repos, dbPrefix)

    # Install vizGrinmoire packages needed, from their git repos
    if not args.noinstvg:
        install_from_git (vgConf["dir"],
                          ["vizGrimoireJS", "vizGrimoireUtils"],
                          vgConf)

    # Run unique_ids and affiliation (people stuff)
    # except that --nopeople was specified
    if not args.nopeople:
        unique_ids (dbPrefix)
        affiliation (dbPrefix)

    # Install vizgrimoire R package and its R dependencies, just in case
    if not args.noinstvgr:
        create_rlib (rConf["libdir"])
    if not args.noinstvgr and not args.nordep:
        install_rdepend (rConf["libdir"], rConf["vgrpkg"])
    if not args.noinstvgr:
        install_vizgrimoirer (rConf["libdir"], rConf["vgrpkg"])

    # Install Python dependencies
    if not args.nopythondep:
        install_pdepend (pythonConf["libdir"], pythonConf["libs"])


    if not args.noanalysis:
        run_analysis ([glConf["scm-analysis"], glConf["its-analysis"]],
                      [dbPrefix + "_" + "cvsanaly", dbPrefix + "_" + "bicho"],
                      [dbPrefix + "_" + "cvsanaly", dbPrefix + "_" + "cvsanaly"],
                      JSONdir)

    if not args.nobrowser:
        produce_dashboard (vizgrimoirejs_dir = \
                               os.path.join(vgConf["dir"],
                                            vgConf["vizGrimoireJS"]["dir"]),
                           example_dir = my_dir,
                           dashboard_dir = dashboard_dir,
                           json_dir = JSONdir)
