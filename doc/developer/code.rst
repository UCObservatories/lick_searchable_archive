Source Code Organization
========================

config
------
The ``config`` directory holds configuration files that will be deployed.

doc
---
The Sphinx documentation.

host_vars
---------
The Ansible by host configuration values. See :ref:`deployemnt`.

lick_archive
------------
This hosts the common Python software in a ``lick_archive`` package.

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

unit_test
---------
The unit tests.



