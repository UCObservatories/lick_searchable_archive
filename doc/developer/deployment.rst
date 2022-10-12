.. _deployment:

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

Ansible Inventories
--------------------

Ansible playbooks can deploy to a specific inventories. For the Lick Searchable Archive we use this
ability to distinguish between a "development" and a "ops" environment. For example the inventory 
for ops is::

    [dbservers]
    quarry.ucolick.org
    [appservers]
    quarry.ucolick.org

``dbservers`` in the above inventory is used when deploying the database.
``appservers`` is used to deploy the python scripts and applications. Theoretically different 
machines could be used for both but that isn't supported yet.

Ansible ``host_vars``
---------------------
Configuration for an environment can be set in the ``host_vars`` directory. For example there's a
file named ``host_vars/quarry.ucolick.org`` for the ops environment::

    db_data_device: /dev/sdh
    postgres_version: 12
    archive_nfs_source: legion:/data/mthamilton
    archive_nfs_uid: 1009
    archive_nfs_gid: 1039    

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
    ids for consistency with other UCO machines.

Ansible is Declaritive
----------------------
Ansible is a declaritive tool: you define how the system *should* be and it tries to make that happen.
This means it will only run changes that are needed. For example for database
servers, if PostgreSQL is already installed, it will not re-install it.  
Running ansible twice is safe and will just verify that everything worked.

Deploying the database
----------------------

To deploy the database to a target machine that meets the :ref:`deploy-requirements` use the
following::
    
    ansible-playbook  dbservers.yml -i  your_env -u your_user -K

`your_env` in the above command is the inventory file described earlier that lists the machines
being deployed to.  `your_user` is a user on the target machine with sudo access that can be
ssh'd into without a password.

The ``--key-file`` argument can be used to specify a specific ssh key when connecting to the target
machine.  For example this is how I deploy in ops::

    ansible-playbook  dbservers.yml -i  ops --key-file ~/work/keys/id_quarry_localdusty  -u localdusty -K

This playbook will prompt you for the user's sudo password. It will then do the following (if needed):

* Install PostgreSQL
* Make an ext4 file system for the database storage, and mount it as ``/pg_data``.
* Setup the ``archive_db`` database cluster, ``archive`` database, and ``archive`` database user.
* Install the automatic backup cron jobs and backup scripts.
* Setup the NFS mount to the archive's file storage as ``/data``.

Deploying the software
----------------------

To deploy Archive's Python software to the target machine::

    ansible-playbook  apservers.yml -i your_env -u your_user -K



Post Deployment Steps
---------------------

Fresh machine:
create schema *todo link to db docs*
python /opt/lick_archive/bin/create_schema.py # Can't run directly, shebang is wrong.

Create django db.
cd /opt/lick_archive/lib/python3.9/site-packages/lick_searchable_archive
sudo -u archive /opt/lick_archive/bin/python manage.py migrate



