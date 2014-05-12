
# Analysis of GitHub repositories using Grimoire tools

How to run to get a dashboard with all projects in organization MetricsGrimoire:

vg-github.py --user jgb --passwd XXX --dir /tmp/vgr --removedb
 --ghuser ghuser --ghpasswd XXX MetricsGrimoire

How to run to get a dashboard with all projects in organization MetricsGrimoire and VizGrimoire:

vg-github.py --user jgb --passwd XXX --dir /tmp/vgr --removedb
 --ghuser ghuser --ghpasswd XXX MetricsGrimoire VizGrimoire

How to run to get a dashboard with all projects from user jgbarah:

vg-github.py --user jgb --passwd XXX --dir /tmp/vgr --removedb
 --ghuser ghuser --ghpasswd XXX jgbarah

How to run to get a dashboard with project VizGrimoire/GrimoireLib:

vg-github.py --user jgb --passwd XXX --dir /tmp/vgr --removedb
 --ghuser ghuser --ghpasswd XXX jgbarah VizGrimoire/GrimoireLib

More info:

vg-github.py --help
