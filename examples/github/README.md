
# Analysis of GitHub repositories using Grimoire tools

* How to get a dashboard in the directory /tmp/mg analyzing all projects in organization MetricsGrimoire of GitHuB:

```
build_dashboard.py --user jgb --passwd XXX --dir /tmp/mg --removedb \
 --ghuser ghuser --ghpasswd XXX MetricsGrimoire
```

[jgb / XXX are the MySQL database user / password, ghuser / XXX are the GitHub user / password]
 
* How to get a dashboard in the directory grimoire-dashboard of the personal home directory, with all projects in organization MetricsGrimoire and VizGrimoire, using the name Grimoire:

```
build_dashboard.py --user jgb --passwd XXX --dir ~/grimoire-dashboard \
 --removedb --ghuser jgbarah --ghpasswd XXX MetricsGrimoire VizGrimoire \
 --name Grimoire
```

* How to get a dashboard with all projects from user jgbarah:

```
build_dashboard.py --user jgb --passwd XXX --dir /tmp/jgbarah --removedb \
 --ghuser ghuser --ghpasswd XXX jgbarah
```

* How to get a dashboard with project VizGrimoire/GrimoireLib:

```
build_dashboard.py --user jgb --passwd XXX --dir /tmp/vgr --removedb \
 --ghuser ghuser --ghpasswd XXX jgbarah VizGrimoire/GrimoireLib
```

More info:

```
build_dashboard.py --help
```
