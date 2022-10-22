TODO
====
Here are various to do items for the Lick Searchable Archive, in no particular order.

Deployment Todo
---------------

* Deploy blank dbsqlite3 for Django, or switch django to using postgres.
* Playbook for deploying a development node?
* Playbook for setting up an admin user on the archive host.
* Deploy in Pythoh packages? Debian Packages for ingest_watchdog? I think at least the common code
  in lick_archive should be packaged up.
* Deploy rotating logging handlers for celery
* Clean up the sections in ops/development inventories.
* Pull mounting the archive file system into its own role?

Code Quality
------------
* make sure i'm consistent in os.path vs Path usage.
* Improve comments!
* docstring comments for API docs
* Use /usr/bin/env python in shebang
* Reorganize code so ansible is in its own deploy directory, and source is under src?

Testing Todo
------------
* A tool to replay an existing days worth of metadata for testing live ingest performance.
* expand automated testing beyond just metadta mappings


Administration Todo
-------------------
* Monitor scripts to send e-mails when something's down
* Statistics on ingest, queries, downloads
* Change the database back ups to record enough information to re-use the existing database device.
