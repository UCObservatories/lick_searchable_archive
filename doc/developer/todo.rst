TODO
====
Here are various to do items for the Lick Searchable Archive, in no particular order.

Deployment Todo
---------------

* Run django migrate against the archive_django db
* Playbook for deploying a development node?
* Playbook for setting up an admin user on the archive host.
* Deploy in Python packages? Debian Packages for ingest_watchdog? I think at least the common code
  in lick_archive should be packaged up.
* Switch logging to syslog/rsyslog and logrotate
* Pull mounting the archive file system into its own role?
* Figure out self signed keys for having the ingest_watchdog deployed on the storage host
* Figure out if the ignest_watchdog will ever be used remotely, if not it can
  be greatly simplified.
* Delete code prior to copying, or sync via rsync?
* Auto deploy django/metadata dbs fs the tables aren't there?
Code Quality
------------
* make sure i'm consistent in os.path vs Path usage.
* Improve comments!
* docstring comments for API docs
* Use /usr/bin/env python in shebang
* Reorganize code so ansible is in its own deploy directory, and source is under src?
* Get pid into logs for searching journalctl
* get stdout into logs?
* A data dictionary class usable by the db schema, api, clients and for generating docs? See field_info in archive_schema
  - Once this is done, remove sqlalchemy from packages installed with "common"
* Make date format returned by api consistent, easy to parse and document.
* At one point I decided to separate the query/query_api.py classes from the lick_archive classes, and create a distinct QueryAPIView as a base
  class. Given how things have turned out I'm not sure that's necessary. Maybe it could be merged with query/views.py
* I passed in the instrument as a "filter", I don't really like that.
  I'd like the api to accept any field as a "filter", but to do that the
  api validation couldn't use a serializer like it does now. Also there'data
  have to be a fancy frontend to add new filter.
* Have a "http" debug level that spews out header/cookie/session info on requests, but
  is disabled by default to stop spewage to logs? Or is "debug" enough.

Additional Features
-------------------
Add file size to db.
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
* ingest_watchdog is leaving a lot of logs around, should logging be configured to delete old ones?