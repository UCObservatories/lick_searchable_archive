TODO
====
Here are various to do items for the Lick Searchable Archive, in no particular order.

Deployment
----------

* Deploy to ``venv`` virtual environment
* Custom settings per site (ops / develop) 
* Regenerate key in django settings in ansible. Make sure any keys in any other configs
  that are in git aren't actually used in ops.
* Do I want proper settings.conf file for django side or just use settings.py?
* Deploy blank dbsqlite3 for Django, or switch django to using postgres.
* Playbook for deploying a development node?
* Playbook for setting up an admin user on the archive host.
* Separate DB, ingest, ingest_watchdog playbooks.
* Common ansible stuff in roles
* Deploy ingest_watchdog as a service
* Deploy gunicorn, celery, redis.
* Deploy in Pythoh packages? Debian Packages for ingest_watchdog? I think at least the common code
  in lick_archive should be packaged up.

Code Quality
------------
* make sure i'm consistent in os.path vs Path usage.
* Improve comments!
* docstring comments for API docs

Testing
--------
* A tool to replay an existing days worth of metadata for testing live ingest performance.


Administration
--------------
* Monitor scripts to send e-mails when something's down
* Statistics on ingest, queries, downloads
