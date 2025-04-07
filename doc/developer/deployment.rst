.. _deployment:

Deployment
==========

The lick searchable archive is deployed using `Ansible <https://www.ansible.com/>`_. This requires
a development/deployment machine that has ansible on it.

Setting up a Development/Deployment Machine
-------------------------------------------
I used conda to setup my personal machine to create a distinct python environment::

    conda create -n archive python=3.12
    conda activate archive

    *or*

    python3.12 -m venv archive
    source archive/bin/activate

Then install packages needed for unit testing::

    pip install pytest
    pip install django
    pip install astropy
    pip install sqlalchemy
    pip install djangorestframework
    pip install psycopg2-binary
    pip install tenacity
    pip install coverage

Packages needed for external tests (in test/ext_test)::

    pip install requests

Packages needed for building the frontend
    TODO

Packages needed for deploying::

    sudo apt install ansible

Packages needed for building developer docs::

    pip install sphinx


In addition the following packages are not used on the development machine, but might be good to install
to keep IDEs looking for imports happy::

    pip install Celery

QoL packages:
    pip install ipython

.. _deploy-requirements:

Requirements for deploying the archive
--------------------------------------
    * A host for the archive software
  
      * This host must use Ubuntu 24.04 as its OS.
      * This host must have a user, with sudo access, that can ssh to the target machine without being prompted for a password.
      * The software host requires a database data partition of at least 128GiB (Which will be formatted and mounted during the deployment).
      * KROOT and LROOT must be installed on the software host.

       
    * A host providing the archive data

      * The archive data must be exported to the software host via NFS. Only read-only access is required. The ansible deployment
        will update the archive host's ``/etc/fstab`` to mount it.
    


Configuring the Deployment
--------------------------
The ansible deployment is configured using inventory files and host_vars. If needed default variables can
also be changed. See :ref:`configuration` for detailed descriptions of these files.


Schedule Database User
^^^^^^^^^^^^^^^^^^^^^^
To avoid putting database password information into source code, the schedule database
user information must be manually set in a separate text file stored in the archive configuration directory
(typically '/opt/lick_archive/etc', but see Ansible defaults below for how to change this)::

    $ cat '<user_name>:<password>' > /opt/lick_archive/etc/sched_db_user_info.txt

Building the Frontend
---------------------
Before deploying the archive, the frontend must be built:

    $ cd lick_searchable_archive/frontend
    make all


Deploying
---------
Ansible is a declaritive tool: you define how the system *should* be and it tries to make that happen.
This means it will only run changes that are needed. For example for database
servers, if PostgreSQL is already installed, it will not re-install it.  
Running ansible twice is safe and will just verify that everything worked. This deploy process
is controlled by the following Ansible playbooks.

``single_host_archive.yml``
  Installs everything in a single machine configuration.

``dbservers.yml``
  Installs the PostgreSQL database software and related administrative software.

``django_apps.yml``
  Installs the Django applications and the related services they rely one. (Celery, Gunicorn, etc).

``local_watchdog.yml``
  Installs the ingest_watchdog on the same machine as the rest of the archive.

``remote_watchdog.yml``
  Installs the ingest_watchdog on a machine separate from the rest of the archive.


Deploying everything to a single machine
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deploying everything to a single machine can be done in one command::

    ansible-playbook  single_host_archive.yml -i  your_env -u your_user -K

`your_env` in the above command is the inventory file described earlier that lists the machines
being deployed to.  `your_user` is a user on the target machine with that meets the requirements
stated in :ref:`deploy-requirements`. This will prompt you for the password ``your_user`` uses 
for sudo access on the target machine.

For example::

    ansible-playbook  single_host_archive.yml -i  ops -u localdusty -K

``your_user`` should be able to  ssh to the target machine without being challenged for a password. 
If neccessary it is possible to specify a specific ssh key to use for this. For example::

    ansible-playbook  single_host_archive.yml -i ops --key-file ~/work/keys/id_quarry_localdusty -u localdusty -K

This is all that is needed to deploy the archive software. However there are additional playbooks that can
be used to only deploy parts of the archive.

Deploying the database
^^^^^^^^^^^^^^^^^^^^^^

To deploy only the database to a target environment use the following::
    
    ansible-playbook  dbservers.yml -i  your_env -u your_user -K

This playbook will do the following (if needed):

* Create a Python virtual environment for the archive Python software.
* Install the common archive Python packages and setup the service user and group.
* Install PostgreSQL
* Make an ext4 file system for the database storage, and mount it as ``/pg_data``.
* Setup the ``archive_db`` database cluster, ``archive`` database, and ``archive`` database user.
* Install the automatic backup cron jobs and backup scripts.
* Setup the NFS mount to the archive's file storage as ``/data``.

Deploying the Django applications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To deploy only the Django apps to a target environment, use the following::

    ansible-playbook  django_apps.yml -i your_env -u your_user -K

This play book will do the following (if needed):

* Create a Python virtual environment for the archive Python software.
* Install the common archive Python packages and setup the service user and group.
* Install Redis
* Celery, Gunicorn, Django and other related Python packages into the archive
  virtual environment.
* Setup the configuration and logging for Django, Celery and Gunicorn.
* If on ops, create a new Django secret key file.
* Create systemd services for Celery and Gunicorn.
* Start the Celery and Gunicorn services.

Deploying the ingest_watchdog service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ingest_watchdog service can be deployed in two configurations: local to the rest of the
archive software or separately on the storage server hosting the lick archive file system.

To deploy locally with the rest of the archive services, use the following::

    ansible-playbook  local_watchdog.yml -i your_env -u your_user -K

This play book will do the following (if needed):

* Create a Python virtual environment for the archive Python software.
* Install the common archive Python packages and setup the service user and group.
* Copy the ingest_watchdog script the archive's Python virtual environment.
* Setup logging and users for the ingest_watchdog.
* Configure the ingest_watchdog to poll the archive file system for new files.
* Configure the ingest_watchdog to communicate with the local Django apps.
* Create a systemd service to run the ingest_watchdog script, and start that service.

To deploy the ingest_watchdog separately on the storage server, use the following::

    ansible-playbook  remote_watchdog.yml -i your_env -u your_user -K

This play book will do the following (if needed):

* Create a Python virtual environment for the archive Python software.
* Install the common archive Python packages and setup the service user and group.
* Copy the ingest_watchdog script to the archive's Python virtual environment.
* Setup logging and users for the ingest_watchdog.
* Configure the ingest_watchdog to use the inotify Linux api to find new files.
* Configure the ingest_watchdog to communicate with the remote Django apps.
* Create a systemd service to run the ingest_watchdog script, and start that service.

Post Deployment Steps
---------------------

Backend Host
^^^^^^^^^^^^^

Metadata Database
+++++++++++++++++
On a fresh environment, the deployment will create the archive database but will not create the schema.
This is to allow the administrator to restore a previous database or create a new one. 
See :ref:`Database Administration <db_admin>` for how to do this.


Django Database
+++++++++++++++
The Django environment will also need to be created. Use these commands to do so::

    $ source /opt/lick_archive/bin/activate
    $ cd /opt/lick_archive/lib/python3.10/site-packages/lick_searchable_archive

    # For new database only
    $ ./manage.py makemigrations archive_auth
    $ ./manage.py makemigrations ingest

    # For both new and updated
    $ ./manage.py migrate

Admin Superuser
+++++++++++++++
An admin superuser account should be created on a fresh installation of the archive::

    $ source /opt/lick_archive/bin/activate
    $ cd /opt/lick_archive/lib/python3.10/site-packages/lick_searchable_archive
    $ ./manage.py createsuperuser

