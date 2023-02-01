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
* Figure out proprietary requirements for initial query deployment
Code Quality
------------
* make sure i'm consistent in os.path vs Path usage.
* Improve comments!
* docstring comments for API docs
* Use /usr/bin/env python in shebang
* Reorganize code so ansible is in its own deploy directory, and source is under src?
* Get pid into logs for searching journalctl
* get stdout into logs?

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
