Maintenance
===========

User setup
----------

An admin user for the archive should have sudo access and be in the ``mhdata`` group. 
Before running any maintenance scripts they should switch to that group and also activate the archive's 
python virtual environment::

    $ newgrp mhdata
    $ source /opt/lick_archive/bin/activate

Checking Archive Status
------------------------

To check if the archive is up::

    $ sudo systemctl status "postgre*"
    $ sudo systemctl status redis
    $ sudo systemctl status apache2
    $ sudo systemctl status gunicorn
    $ sudo systemctl status celery
    $ sudo systemctl status ingest_watchdog

To check that ingests have been working::

    $ ingest_stats_by_date.py *month*

If any of returned lines show ``MISMATCH``, it means the number of files in the 
archive's file system do not match what is in the database. This could be 
innocuous if the file isn't something that shouldn't be ingested, or could
be a failure in the ingest workflow.

To check that there are no files owned by ``UNKNOWN``::

    echo "select count(*) from file_metadata where public_date = '9999-12-31';" | psql -U archive -f -

TODO: expand this and what to do if any are found. Also, should this be a script.

To check if the user sync cronjob is running correctly::

    $ cd /var/log/lick_archive/
    $ tail sync_archive_users.log 
    INFO     2025-04-07 20:27:02.757 pid:83391 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.169757.
    INFO     2025-04-07 20:29:03.748 pid:1905 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.424975.
    INFO     2025-04-07 20:31:03.165 pid:2022 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.183701.
    INFO     2025-04-07 20:33:02.574 pid:2045 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.156700.
    INFO     2025-04-07 20:35:03.003 pid:2077 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.196797.
    INFO     2025-04-07 20:37:02.537 pid:2101 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.175910.
    INFO     2025-04-07 20:39:02.998 pid:2366 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.183328.
    INFO     2025-04-07 20:41:02.413 pid:2624 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.184010.
    INFO     2025-04-07 20:43:02.805 pid:2644 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.155678.
    INFO     2025-04-07 20:45:02.205 pid:2664 sync_archive_users:main:138 Completed syncing users. Duration: 0:00:00.166640.

To check if the database backups are running correctly::

   $ sudo ls -lrt /pg_data/backups/

Make sure there is a recent backup named ``archive_db_YYYYMMDD_HHMM.dump.gz`` and ``archive_db_django_20250407_1500.dump.gz``.

Note that these database backups are intended to protect against administrator error. For disaster recover it's expected that the
the VM is being backed up by system like Veeam.