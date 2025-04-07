Todo List
=========
Here are various to do items for the Lick Searchable Archive, in no particular order.
See :ref:`architecture_simplification` for additional larger scale cleanup that could
be performed. 


Deployment Todo
---------------

* Run django migrate against the archive_django db
* Playbook for deploying a development node?
* Document django db stuff with database, include manually resetting everything.
* Deploy in Python packages? Debian Packages for ingest_watchdog? 
* Delete code prior to copying, or sync via rsync?
* Auto deploy django/metadata dbs fs the tables aren't there?
* common_sw role deploys the entire lick_archive directory, it should 
  exclude portions installed by other roles.

Code Quality
------------

* make sure i'm consistent in os.path vs Path usage.
* Improve comments!
* python type annotations!
* token based authentication for api access?
* Update ingest_watchdog to use new configuration class.
* Improve script consistency regarding main() and get_parser. 
* Cleanup script argparse help output
* Cleanup duplicated resync logic between ingest task (when override.access is run) and resync_utils module/scripts.
* Cleanup override access, we currently keep all versions in db, but only need the latest. Do we really need the two
  versions of the override access file class?


API Cleanup
-----------
* docstring comments for API docs
* Make date format returned by api consistent, easy to parse and document.
* I passed in the instrument as a "filter", I don't really like that.
  I'd like the api to accept any field as a "filter", but to do that the
  api validation couldn't use a serializer like it does now. Also there'data
  have to be a fancy frontend to add new filter.
* Support `IVOA TAP <https://www.ivoa.net/Documents/TAP/20190927/index.html>` api?
  
Additional Features
-------------------
* Split file path and filename in db.
* Object query could ignore whitespace

Testing Todo
------------
* The ``ext_test`` tests don't work without the backend API being entirely exposed.
* fuzz/other security testing

Administration Todo
-------------------
* Monitor scripts to send e-mails when something's down
* Statistics on ingest, queries, downloads
* Change the database back ups to record enough information to re-use the existing database device.
