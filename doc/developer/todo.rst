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
* Deploy rotating logging handlers for celery
* Pull mounting the archive file system into its own role?
* Figure out self signed keys for having the ingest_watchdog deployed on the storage host
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
* Consolidate query validation so it isn't done multiple times in query_api. Can it also be shared with lick_archive_client query_page?
* A data dictionary class usable by the db schema, api, clients and for generating docs? See field_info in archive_schema

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