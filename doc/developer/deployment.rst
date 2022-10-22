Deployment
==========

The lick searchable archive is deployed using `Ansible <https://www.ansible.com/>`_. This requires
a development/deployment machine that has ansible on it.

Setting up a Development/Deployment Machine
-------------------------------------------
I used conda to setup my personal machine to create a distinct python environment::

    conda create -n archive python=3.9
    conda activate archive
    pip install sqlalchemy
    pip install psycopg2-binary
    pip install pytest
    pip install coverage
    pip install sphinx
    sudo apt install ansible

In addition the following packages are used on the development machine, but might be good to install
to keep IDEs looking for imports happy::

    pip install Celery
    pip install astropy
    pip install django
    pip install django-rest-framework


.. _deploy-requirements:

Requirements for deploying to a machine
---------------------------------------
    * The machine must use Ubuntu as its OS
    * You must have a user that can ssh to the target machine without being prompted for a password.
    * That user must have sudo acceess.
    * A database data partition. (Which will be formatted and mounted during the deployment).
    * Permission to NFS mount the archive file storage.

For example, for the current ``Quarry.ucolick.org`` machine I've been using the ``localdusty`` account.

Configuring the Deployment
--------------------------
The ansible deployment is configured using inventory files and host_vars. If needed default variables can
also be changed.

Ansible Inventories
^^^^^^^^^^^^^^^^^^^

Inventory files control where Ansible deploys to. For the Lick Searchable Archive we use an "ops" file to define the ops environment.
Other names can be used for development environments.  The ops environment has less debugging information configurated
than development environments.

For ops the current inventory is::

    [dbservers]
    quarry.ucolick.org

    [appservers]
    quarry.ucolick.org

    [ingest_servers]
    quarry.ucolick.org

    [watchdog]
    quarry.ucolick.org

This is for a single machine configurations. 
Theoretically different machines could be used for all of these sections but currently
only deploying the ingest_watchdog to a different machine via the ``watchdog`` section is supported.

Ansible ``host_vars``
^^^^^^^^^^^^^^^^^^^^^
Configuration for a specific machine can be set in a file in the ``host_vars`` directory. For example there's a
file named ``host_vars/quarry.ucolick.org`` for the ops environment::

    db_data_device: /dev/sdh
    postgres_version: 12
    archive_nfs_source: legion:/data/mthamilton
    archive_nfs_uid: 1009
    archive_nfs_gid: 1039
    archive_data_root: /data/data
    archive_data_mount: /data
    archive_nfs_name: mhadmin
    archive_nfs_group: mhdata

``db_data_device``
    This is the device that the database storage is available at. Deployment will create a new
    file system for this device, and will mount it to ``/pg_data``.

``postgres_version``
    This is the version of postgres being used. For Ubuntu 20.04 LTS the correct value is "12".
    For Ubuntu 22.04 LTS it's "14".

``archive_nfs_source``
    This is the NFS source used to NFS mount the archive's storage. It is added to the fstab to
    mount the storage when the machine boots.

``archive_nfs_uid`` and ``archive_nfs_gid``
    The UID and GID of files stored in the archive storage. Deployment will create users with these
    ids.

``archive_data_root``
    This is the path to the root directory of the data files stored in the archive file system.

``archive_data_mount``
    This is the path to the archive file system is mounted to. This may not be the same as ``archive_data_root`` if
    there is additional non archive data on that file system.

``archive_nfs_name`` and ``archive_nfs_group``
    The user and group names that should own the archive file system NFS mount. They are assigned to ``archive_nfs_uid``
    and ``archive_nfs_gid`` respectively.

Ansible defaults
^^^^^^^^^^^^^^^^
The default values for variables used by the Ansible scripts are stored in ``roles/common/defaults/main.yml``. They
can be overridden by variables in host_vars, or be changed directly before deploying.

``venv_root``
    The root directory where lick archive software is installed. Defaults to ``/opt/lick_archive``

``archive_servie_user`` and ``archive_service_group``
    The user and group used to run archive services including the ingest_watchdog and the django applications. 
    Defaults to ``archive``.

``python_version``
    The version of Python in use. Defaults to ``3.9``

``python_install_dir``
    Where Python packages are installed. Defaults to ``{{ venv_root }}/lib/python{{ python_version }}/site-packages``.

``archive_log_dir``
    Where archive software will place any logs. Defaults to ``{{ venv_root }}/var/log``.

``archive_config_dir``
    Where configuration files for archive software are kept. Defaults to ``{{ venv_root }}/etc``.

``ingest_port``
    The port the ingest_watchdog uses to connect to the ingest Django application. Defaults to ``8000``

``watchdog_config``
    The name of the configuration file for the ingest_watchdog. Defaults to ``ingest_watchdog.conf``.

``django_settings``
    The name of the Django settings file. Defaults to ``settings.py``.

``django_secret_keyfile``
    The name of the file storing Django's secret key. This is only created in ops. Defaults to: ``{{ archive_config_dir }}/secret_key``.

``django_log``
    The name of the log file for Django apps. Defaults to ``{{ archive_log_dir }}/lsa_apps.log``

``gunicorn_log``
    The name of the gunicorn log file. Defaults to ``{{ archive_log_dir }}/gunicorn.log``

``redis_url``
    The URL for connecting to Redis. Used by Celery.  Defaults to ``redis://localhost:6379/0``

``supported_instrument_dirs``
    The currently supported instrument directories. Defaults to ``['AO', 'shane']``


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

    ansible-playbook  single_host_archive.yml --key-file ~/work/keys/id_quarry_localdusty -u localdusty -K

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

On a fresh environment, the deployment will create the archive database but will not create the schema.
This is to allow the administrator to restore a previous database or create a new one. 
See :ref:`Database Administration <db_admin>` for how to do this.

The Django environment will also need to be created. Use these commands to do so::

    $ cd /opt/lick_archive/lib/python3.9/site-packages/lick_searchable_archive
    $ sudo -u archive /opt/lick_archive/bin/python manage.py migrate
