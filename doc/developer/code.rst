Source Code Organization
========================

doc
---
The Sphinx documentation.

host_vars
---------
The Ansible by host configuration values. See :ref:`deployment`.

lick_archive
------------
This hosts the common Python software in a ``lick_archive`` package.

roles
-----
The ansible roles containing tasks shared between playbooks.

lick_archive/db
---------------
The code directly accessing and setting up the database.

lick_archive/metadata
---------------------
The code related to reading metadata from archvie files.

lick_searchable_archive
-----------------------
The Django project folder for the archive, in the standard Django layout.

scripts
-------
Command line scripts.

scripts/admin_scripts
---------------------
Command line scripts that are installed during deployment for administrating the archive.

scripts/postgres_scripts
------------------------
Command line scripts specifically for the postgres user.

unit_test
---------
The unit tests.



