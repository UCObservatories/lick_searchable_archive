TODO
====
Here are various to do items for the Lick Searchable Archive, in no particular order.

Deployment Todo
---------------

* Run django migrate against the archive_django db
* Playbook for deploying a development node?
* Playbook for setting up an admin user on the archive host.
* Document django db stuff with database, include manually resetting everything.
* Deploy in Python packages? Debian Packages for ingest_watchdog? I think at least the common code
  in lick_archive should be packaged up.
* Figure out self signed keys for having the ingest_watchdog deployed on the storage host
* Delete code prior to copying, or sync via rsync?
* Auto deploy django/metadata dbs fs the tables aren't there?

Code Quality
------------

* make sure i'm consistent in os.path vs Path usage.
* Improve comments!
* python type annotations!
* Figure out if the ignest_watchdog will ever be used remotely, if not it can
  be greatly simplified. If it is used remotely, could token based authentication work to
  replace it's current "counts" query API?
* token based authentication for api access?
* Use /usr/bin/env python in shebang
* Reorganize code so ansible is in its own deploy directory, and source is under src?
  - Create roles for app specific install stuff that can be imported, ala services
  - Put services in their own source, currently job_queue and ingest_watchdog
* Have a "http" debug level that spews out header/cookie/session info on requests, but
  is disabled by default to stop spewage to logs? Or is "debug" enough.
* Update ingest_watchdog to use new configuration class.
* Improve script consistency regarding main() and get_parser. 
* Cleanup script argparse help output

API Cleanup
-----------
* docstring comments for API docs
* Make date format returned by api consistent, easy to parse and document.
* I passed in the instrument as a "filter", I don't really like that.
  I'd like the api to accept any field as a "filter", but to do that the
  api validation couldn't use a serializer like it does now. Also there'data
  have to be a fancy frontend to add new filter.
* Remove archive_root from any paths returned by the API.

Bugs
----
* When using separate frontend and backend nodes, header requests won't see the
  header information of proprietary data even if user is logged in.
* Enabling/disabling result/query fields results in original/default values in disabled fields not being re-populated
  after submitting a query.  (Disabled fields not passed to view and so it can't pass what was in the
  field before query request).

Additional Features
-------------------
Split file path and filename in db.
object search could ignore whitespace

Testing Todo
------------
* A tool to replay an existing days worth of metadata for testing live ingest performance.
* Tools to sanity check live system
* fuzz/other security testing

Administration Todo
-------------------
* Monitor scripts to send e-mails when something's down
* Statistics on ingest, queries, downloads
* Change the database back ups to record enough information to re-use the existing database device.
