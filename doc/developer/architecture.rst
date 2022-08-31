Lick Archive Arctecture
=======================

.. image:: /_static/draft_archive_architecture.png

The above (hurriedly drawn) diagram shows the architecture for the Lick Searchable Archive.

Quarry
    The current host VM for the Lick Searchable Archive's database an software. It NFS mounts the
    archive data from the Storage Host (currently Legion). In theory the archive could be split 
    between multiple machines (for example a downloading VM, query VM, database VM, and ingest VM)
    but more than the load will not be large enough to justify that.

WSGI Web Server
    A WSGI Web Server interfaces between the UCO's webserver and Django Python code. The static 
    parts of the searchable archive (static html and images, and the data itself) is sent from the
    main UCO webserver (currently running Apache). The Django apps will handle any dynamically 
    generated pages (query resutls) and access control for proprietary data. Currently 
    Gunicorn is being used.

Ingest Watchdog Service
    This is a Python application that will run as a Linux systemd service. It's job is to monitor
    the archive NFS mounts for new data, and notify the Metadata Ingest REST API of the new file.
    In "polling" mode it scans the NFS mount periodically and can run on Quarry. In "inotify" mode
    it must run on the Storage Host to be notified of new files by Linux.

Metadata Ingest REST API
    A Django application that receives notifications of new file  and starts a task in Celery to
    read the metadata from the file and insert it into the Postgres database. This API is only
    used internally and is not planned to be accessible to the internet.

Query REST API
    A Django application that receives query requests from clients and returns those results. It
    will handle user authentication for access to data in its proprietary access period.

Download REST API
    A Django application that allows for downloading single files or zip/tar archives of multiple
    files in the archive. A request for multiple files will be sent to Celery to be handled as a task.
    It will handle user authentication for access to data in its proprietary access period.

Scripts
    The standalone scripts use to administer the archive, including ingesting metadta for files in bulk.

Celery/Redis Task Queues
    Celery is used to handle tasks that take too long to be handled during a web request. 
 
Major Software Dependencies
---------------------------

1. `PostgreSQL <https://www.postgresql.org/>`_
    The metadata for the files in the Searchable Archive is stored here for querying. The metadata used by Django Apps will also be stored here.

2. `Django <https://www.djangoproject.com/>`_
    This python package is the framework for the applications that will implement the web APIs in the Searchable Archive.

3. `Django Rest Framework <https://www.django-rest-framework.org/>`_
    Helps make the web APIs.

4. `Celery <https://docs.celeryq.dev/en/stable/index.html>`_
    Celery is a task queue management system used to handle slow background tasks that should not hold up HTTP requests from clients. 

5. `Redis <https://redis.io/>`_
    Redis is an in memory database used as a message broker by Celery.

6. `Gunicorn <https://gunicorn.org>`_
    A WSGI webserver that bridges between Apache and python applications.

7. `SQLAlchemy <https://www.sqlalchemy.org/>`_
    A Python database toolkit for accessing the database. We use its ORM to define the database's
    schema and to access the database without having to hardcode SQL.

8. `Astropy <https://www.astropy.org/>`_
    A Python package for astronomy software. We use it for reading FITS files.

9. `pgspehre <https://github.com/akorotkov/pgsphere>`_
    A PostgreSQL plugin for Spherical Coordinates. We use it for fast indexed queries on ra/dec. The 
    main github link for this is out of date, but I couldn't find a better one. But Debian some
    how gets new updates in its packages? 

10. `Python <https://python.org>`_
     All software is written in Python.



