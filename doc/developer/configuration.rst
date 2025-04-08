.. _configuration:

Archive Configuration
=====================

Deploymen Configuration
-----------------------

.. _inventory:

Ansible Inventories
^^^^^^^^^^^^^^^^^^^

Inventory files control where Ansible deploys to. For the Lick Searchable Archive we use an "ops" file to define the ops environment.
Other names can be used for development environments.  The ops environment has less debugging information configurated
than development environments.

For ops the current inventory is::

    [all:vars]
    archive_config=ops
    remote_watchdog=False 
    # Front end user facing connection info
    frontend_scheme="https"
    frontend_host= "quarry.ucolick.org"

    # API access from frontend/backend
    api_scheme=http
    api_server=localhost
    api_port=8000
    # Where ansible should copy from 
    archive_source_dir=/home/dusty/work/lick_searchable_archive
    # Connection info Lick Observatory schedule database
    schedule_db_host=schedpsql.ucolick.org
    schedule_db_name=info
    # Set this to "restarted" to restart everything after deploy
    # "stopped" to keep the archive down after deploy
    archive_service_state=restarted

    [frontend]
    # Frontend host. In theory this can be separate from the backend but this has not 
    # been tested
    quarry.ucolick.org

    [backend]
    # Backend host
    quarry.ucolick.org

    [backend:vars]
    # Which django apps to install on the backend
    archive_apps=['ingest', 'query', 'archive_admin', 'archive_auth','download']
    # Which systemd services to install on the backend. 
    # 'job_queue' : Installs celery and is used for ingesting metadata
    # 'ingest_watchdog': Installs ingest_watchdog.py which watches the archive NFS mount for new files.
    services=['job_queue', 'ingest_watchdog']
    # The type of host. 
    # `single_host`: Indicates the archive is installed on a single host
    # 'frontend':    Indicates this is the frontend host in a dual host configuration (not tested)
    # 'backend':     Indicates this is the backend host ina dualhost configuration (not tested)
    host_type=single_host
    gshow_path=/opt/kroot/rel/default/bin/gshow
    # Gunicorn settings for frontend
    frontend_proxy_server="localhost"
    frontend_proxy_port=8000

This is for a single machine configuration.  Theoretically different machines could be used for the frontend and backend but this has not been tested.

.. _host_vars:

Ansible ``host_vars``
^^^^^^^^^^^^^^^^^^^^^
Configuration for a specific machine can be set in a file in the ``host_vars`` directory. For example there's a
file named ``deploy/host_vars/quarry.ucolick.org`` for the ops environment::

    db_data_device: /dev/sdh
    postgres_version: 16
    archive_nfs_source: legion:/data/mthamilton
    archive_nfs_uid: 1009
    archive_nfs_gid: 1039
    archive_data_root: /data/data
    archive_data_mount: /data
    archive_nfs_name: mhadmin
    archive_nfs_group: mhdata
    archive_service_group: stuff
    archive_service_uid: 1002
    archive_service_gid: 1001
    webserver_user: www-data
    webserver_group: stuff
    ssl_cert: /etc/ssl/certs/server_cert.pem
    ssl_private_key: /etc/ssl/private/server_privkey.pem

``db_data_device``
    This is the device that the database storage is available at. Deployment will create a new
    file system for this device, and will mount it to ``/pg_data``.

``postgres_version``
    This is the version of postgres being used. For Ubuntu 24.04 LTS the correct value is "16".

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

    ssl_cert: /etc/ssl/certs/server_cert.pem
    ssl_private_key: /etc/ssl/private/server_privkey.pem

``archive_service_group``, ``archive_service_uid``, ``archive_service_gid``
    The user and group to be used for running all services, django apps, and scripts used by the archive.
    This user/group combination should have read permissions to all of the archive data files and directories.

``webserver_user``, ``webserver_group``
    The user and group that the apache virtual host server will run as. This user/group combination 
    should have read permissions to all of the archive data files and directories.

Ansible defaults
^^^^^^^^^^^^^^^^
The default values for variables used by the Ansible scripts are stored in ``deploy/roles/common/defaults/main.yml``. They
can be overridden by variables in host_vars, or be changed directly before deploying.

``venv_root``
    The root directory where lick archive software is installed. Defaults to ``/opt/lick_archive``

``archive_servie_user`` and ``archive_service_group``
    The user and group used to run archive services including the ingest_watchdog and the django applications. 
    Defaults to ``archive``.

``python_version``
    The version of Python in use. Defaults to ``3.12``

``package_install_dir``
    Where Python packages are installed. Defaults to ``{{ venv_root }}/lib/python{{ python_version }}/site-packages``.

``kroot``
   Where KROOT is installed. Defaults to ``/opt/kroot/rel/default/``

``lroot`` 
    Where LROOT is installed defaults to ``/usr/local/lick/``

``archive_log_dir``
    Where archive software will place any logs. Defaults to ``/var/log/lick_archive``.

``archive_config_dir``
    Where configuration files for archive software are kept. Defaults to ``{{ venv_root }}/etc``.

``watchdog_config``
    The name of the configuration file for the ingest_watchdog. Defaults to ``ingest_watchdog.conf``.

``django_settings``
    The name of the Django settings file. Defaults to ``settings.py``.

``django_secret_keyfile``
    The name of the file storing Django's secret key. This is only created in ops. Defaults to: ``{{ archive_config_dir }}/secret_key``.

``django_log``
    The name of the log file for Django apps. Defaults to ``{{ archive_log_dir }}/lsa_apps.log``

``redis_url``
    The URL for connecting to Redis. Used by Celery.  Defaults to ``redis://localhost:6379/0``

``supported_instrument_dirs``
    The currently supported instrument directories. Defaults to ``['AO', 'shane']``

``frontend_url``
    The URL used to access the archvie frontend, based on variables defined in the inventory file. Defaults to ``{{ frontend_scheme }}://{{ frontend_host }}/{{ archive_url_path_prefix }}``

``default_search_radius``
    The search radius around a point when doing a ra/dec query. Defaults to ``1 arcmin``

``sync_users_cron_minute``
    The minutes portion of the cronjob used to sync users from the schedule database to the archive. Defaults to every two minutes, i.e. ``1-59/2``

Runtime Configuration
---------------------
TODO

* archive_config.ini

* ingest_watchdog.conf

* settings.py

* celery.env 

* guniconr.conf.py
