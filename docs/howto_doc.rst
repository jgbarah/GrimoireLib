How to produce documentation using Sphinx
=========================================

If there is no change in the list of modules:

* Fix sys.path.insert in docs/conf.py, by adding the paths that must be included in PYHTONPATH to import all modules needed by those to be documented.

* Run:

::

   cd docs
   make html

This will try to build all HTML content in html directory under BUILDDIR, as defined in docs/Makefile (which should exist). Therefore, change that variable to your taste.

If there are changes in the list of modules:

::

   cd docs
   sphinx-apidoc --force --separate -o . ..
   make html

How to push documentation to GitHub
===================================

The directory where the HMTL content was built should be a clone of the gh-pages branch of the project. So, first clone the project, change to the gh-pages branch, and then run the above commands to populate with the new HTML files. After that:

::

   git add .
   git commit -m "Updating HTML docs" 
   git push origin gh-pages
