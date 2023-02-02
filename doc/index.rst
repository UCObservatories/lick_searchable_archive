.. Lick Observatory Searchable Archive documentation master file, created by
   sphinx-quickstart on Tue Aug 30 13:45:40 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Lick Observatory Searchable Archive's documentation!
===============================================================

.. toctree::
   :maxdepth: 1
   :caption: Developer Documentation

   developer/architecture
   developer/database
   developer/code
   developer/deployment
   developer/todo

.. toctree::
   :maxdepth: 1
   :caption: API Documentation

   schema

.. toctree::
   :maxdepth: 1
   :caption: User Documentation

   user/intro

The Lick Observatory Searchable Archive is a project intended to allow easy searching and retrieval of 
data gathered by the Observatory. It is currently under development.

Goals
------
1 Provide a searchable interface to access to the data in the current Lick Archive.
2 Provide continuing access to data gathered in the future.
3 Respect proprietary access for recently gathered data.


Development Plan
=================
The development of the Searchable Archive is broken up into several phases.

1. Infrastructure (Complete)
2. Catalog Existing of Shane Kast and Shane AO/ShARCS data.(Complete)
3. Catalog new Shane Kast and Shane AO/ShARCS data (Complete)
4. Provide a simple web interface for querying the archive. (Expected end of 2022)
5. Provide Authentication and access to proprietary data (Q1 2023)
6. Provide ability to download data.

After phase 6 the archive will be ready to become publically accessible, although there is be
the potential for future work:

* Add additional instruments.
* Add environmental web cam/sky cam images
* Add api support to astroquery



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
