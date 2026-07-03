.. _archive_api:

Lick Archive API
================

Query
-----
:URL: ``archive/data/?`` *query field* ``=`` *operator* ``,`` *value 1* ``,`` ... *value n* ``&`` *option 1* ``=`` *option value* ... ``&`` *option n* ``=`` *option value*

:Code: lick_archive/apps/query

Queries can be performed with a GET to the query URL. If the data is public, no session cookies are needed.
The queries are built by using a query string as formatted above. 

See TODO for a listing of all the fields and whether they can be queried, sorted on, or returned 
by a query.

Supported query fields
^^^^^^^^^^^^^^^^^^^^^^

+-----------+-----------------------------+---------------+-------------------------------------------------------------+
| field     | Format                      | Allowed       | Example                                                     |
|           |                             | Operators     |                                                             |
+===========+=============================+===============+=============================================================+
| obs_date  | ISO-8601 date or date time  | eq, in        | obs_date=in,2019-05-23T12:00:00-08,2019-05-24T12:00:00-08   |
+-----------+-----------------------------+---------------+-------------------------------------------------------------+
| filename  | "YYYY-MM/DD/INSTR/filename" | eq, sw, in    | filename=sw,2019-05/23/shane                                |
+-----------+-----------------------------+---------------+-------------------------------------------------------------+
| object    | string                      | eq, sw, cn,   | object=cni,feige110                                         |
|           |                             | eqi, swi, cni |                                                             |
+-----------+-----------------------------+---------------+-------------------------------------------------------------+
| coord     | ra/dec and radius           | in            | coord=in,23h19m58.4s,-05d09m56.171s,60s                     |
+-----------+-----------------------------+---------------+-------------------------------------------------------------+

The ``coord`` search finds files with an RA and DEC within the circle on the sky. If the radius is
not given a default value is used.  The coordinates can be specified in multiple formats::

    23h19m58.4s   -05d09m56.171s
    23:19:58.4    -05:09:56.171
    23 19 58.4    -05 09 56.171
    349.99333202  -5.1656031

The radius can be specified using ``h, d, m, s`` for hours, degrees, arcminutes, and arcseconds. The
radius and coordinates are case insensitive.

Query Operators
^^^^^^^^^^^^^^^

+----------+----------------------------------------------+
| Operator | Meaning                                      | 
+==========+==============================================+
| eq       | The DB value exactly matches the given value |
+----------+----------------------------------------------+
| sw       | The DB value starts with the given value     |
+----------+----------------------------------------------+
| cn       | The DB value contains the given value        |
+----------+----------------------------------------------+
| in       | The DB value is contained in the given value |
+----------+----------------------------------------------+


An ``i`` can be appended to ``eq``, ``sw``, and ``cn`` for ``object`` queries for a case insenstiive search.

Query Options
^^^^^^^^^^^^^

``results``
    A comma separated list of result fields. Note that the ``id`` field is always returned. Example: ``results=filename,obs_date,header``

``sort``
    A comma separated list of fields to sort by. A "-" can be prefixed to a field to reverse the sort order. Example: ``sort=-obs_date,frame_type``

``count``
    Tells the query to only return a count of files matching the query. No results are returned.

``filters``
    Applies an additional filter to the query results. Currently only the ``instrument`` field
    can be filtered on. Accepted values are::

        KAST_RED
        KAST_BLUE
        SHARCS
        ALL_SKY
        AO_SAMPLE
        AO_TEL
        APF_CAM
        APF
        APF_GUIDE
        CAT
        GEMINI
        HAM120
        HAM_CAM1
        HAM_CAM2
        NICKEL
        PEAS
        PFCAM
        SKYCAM2

``coord_format``
    Controls how the RA/DEC values in the results are formatted. Allowed values are ``asis``, 
    ``hmsdms``, and ``degrees``. ``asis`` causes the values to be returned exactly as they appaer in the FITS headers.

``page_size``
    Controls how many results are returned in a single page.

``page``
    An integer specifying which page to return from the query. 

Special Result fields
^^^^^^^^^^^^^^^^^^^^^
There are two special result fields in the query API: ``download_link`` and ``header``. ``download_link`` returns a link to
download a single file.  ``header`` returns a link to download the header as an ASCII text file.

Query Results
^^^^^^^^^^^^^

Query results are returned as JSON objects with the following keys.

``count``
    The total number of results returned by the query (not just this page).

``next``
    A link to the next page of query results, or ``null`` if there are no more results.

``previous``
    A link to the previous page of query results, or ``null`` if this is the first page.

``results``
    A list of JSON objects. Each object has a key for each returned result field.
    Every result will contain an ``id`` field giving a unique identifier for the file.
    The ``header`` result field will return a URL that can be used to retrieve the header
    for the file.

Query Examples
^^^^^^^^^^^^^^

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/?obs_date=eq,2019-05-24&results=filename&page_size=5'

.. code-block:: json

    {"count":82,
     "next":"http://localhost:8000/archive/data/?obs_date=eq%2C2019-05-24&page=2&page_size=5&results=filename",
     "previous":null,
     "results":[{"filename":"2019-05/23/shane/b23.fits","id":1648},
                {"filename":"2019-05/23/shane/b12.fits","id":1649},
                {"filename":"2019-05/23/shane/r11.fits","id":1650},
                {"filename":"2019-05/23/shane/r20.fits","id":1651},
                {"filename":"2019-05/23/shane/b4.fits","id":1652}]}
                
In the above query, ``obs_date`` is queried on with ``eq`` as the operator. The ``eq`` operator 
returns an exact match. Note that the date passed was 2019-05-24, but the files appear to be 
from 2019-05-23. That's because query parameters are assumed to be UTC when specified. 
Querying on a range of dates from noon to noon PST is a more intuitive way to query for items 
on a particular day:

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/?obs_date=in,2019-05-23T12:00:00-08,2019-05-24T12:00:00-08&results=filename&page_size=5'

.. code-block:: json

    {"count":70,
     "next":"http://localhost:8000/archive/data/?obs_date=in%2C2019-05-23T12%3A00%3A00-08%2C2019-05-24T12%3A00%3A00-08&page=2&page_size=5&results=filename",
     "previous":null,
     "results":[{"filename":"2019-05/23/shane/b23.fits","id":1648},
                {"filename":"2019-05/23/shane/b12.fits","id":1649},
                {"filename":"2019-05/23/shane/r11.fits","id":1650},
                {"filename":"2019-05/23/shane/r20.fits","id":1651},
                {"filename":"2019-05/23/shane/b4.fits","id":1652}]}

Below is an example coordinate search with an instrument filter:

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/?coord=in,23h19m58.4s,-05d09m56.171s,60s&filters=instrument,KAST_RED,KAST_BLUE&results=filename,object,obs_date&sort=obs_date&page_size=5'  

.. code-block:: json

    {"count":2,
     "next":null,
     "previous":null,
     "results":[{"id":2959,"filename":"2019-05/24/shane/b27.fits","object":"feige110","obs_date":"2019-05-25T11:49:37.620000Z"},
                {"id":2997,"filename":"2019-05/24/shane/r102.fits","object":"feige110","obs_date":"2019-05-25T11:49:40.060000Z"}]}

The four primary fields used for querying can also be combined in a single query, resulting in ANDing the result of both. Below is an example of a combined object and date query:

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/?obs_date=in,2019-05-23T12:00:00-08,2019-05-24T12:00:00-08&object=cni,BD%2B28&results=filename,object&page_size=5' 

.. code-block:: json

    {"count":4,
     "next":null,
     "previous":null,
     "results":[{"filename":"2019-05/23/shane/r34.fits","object":"BD+28 4211","id":108745},
                 {"filename":"2019-05/23/shane/b33.fits","object":"BD+28 4211","id":108747},
                 {"filename":"2019-05/23/shane/r35.fits","object":"BD+28 4211","id":108772},
                 {"filename":"2019-05/23/shane/r33.fits","object":"BD+28 4211","id":108791}]}

Special columns can be used to get links for downloading the file or just its header:

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/?obs_date=eq,2019-05-24&results=filename,header,download_link&page_size=2'

.. code-block:: json

    {"count":81,
     "next":"https://archive.ucolick.org/archive/data/?obs_date=eq%2C2019-05-24&page=2&page_size=2&results=filename%2Cheader%2Cdownload_link",
     "previous":null,
     "results":[{"filename":"2019-05/23/APF/ucb-blo212.fits",
                    "id":138,
                    "header":"https://archive.ucolick.org/archive/data/2019-05/23/APF/ucb-blo212.fits/header",
                    "download_link":"https://archive.ucolick.org/archive/data/2019-05/23/APF/ucb-blo212.fits"},
                {"filename":"2019-05/23/APF/ucb-blo223.fits",
                    "id":141,
                    "header":"https://archive.ucolick.org/archive/data/2019-05/23/APF/ucb-blo223.fits/header",
                    "download_link":"https://archive.ucolick.org/archive/data/2019-05/23/APF/ucb-blo223.fits"}]}

Download Single
---------------
:URL: ``archive/data/`` *filepath*

:Code: lick_archive/apps/download

Files can be downloaded using the ``archive/data/`` URL with the file path and name appended.

Download Single Example
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/2019-05/24/shane/r102.fits' --output-dir ~/Downloads/ --remote-header-name --remote-name
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100 2252k  100 2252k    0     0  33.1M      0 --:--:-- --:--:-- --:--:-- 33.3M

Header
------
:URL: ``archive/data/`` *filepath* ``/header``
:Code: lick_archive/apps/query

The header for a FITS file can be retrieved by appending ``/header`` to the download URL for the file. 

Header Example
^^^^^^^^^^^^^^

.. code-block:: bash

    $ curl 'https://archive.ucolick.org/archive/data/2019-05/24/shane/r102.fits/header' > header.txt
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100 17253  100 17253    0     0   865k      0 --:--:-- --:--:-- --:--:--  886k


Download Multiple
-----------------
:URL: ``archive/api/download``
:Code: lick_archive/apps/download

It is possible to download multiple files combined into a gzipped tar file. This requires sending a JSON list of the files to download. This can be directly using JSON or using a "form" style post. 

Download Multiple via form POST Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ echo 'download_files=["2019-05/23/shane/b1.fits", "2019-05/23/shane/r1.fits","2019-05/23/shane/b2.fits", "2019-05/23/shane/r2.fits"]' > download_form_post_data
    $ curl  --data-binary @download_form_post_data 'https://archive.ucolick.org/archive/api/download' --output-dir ~/Downloads/ --remote-header-name --remote-name

Download Multiple via JSON POST Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ echo '["2019-05/23/shane/b1.fits", "2019-05/23/shane/r1.fits","2019-05/23/shane/b2.fits", "2019-05/23/shane/r2.fits"]' > download_form_json_data
    $ curl  -H 'Content-Type: application/json' --data-binary @download_form_post_data 'https://archive.ucolick.org/archive/api/download' --output-dir ~/Downloads/ --remote-header-name --remote-name


Accessing Proprietary Data
--------------------------

The archive supports two ways of authenticating users in order to view propreitary data.
For automated API access, Basic Authenticatio can be used. For clients with user interaction,
a session based authentication scheme can be used to save user status in a session.

Basic Authentication
^^^^^^^^^^^^^^^^^^^^

For Basic Authentication, the login credentials are sent in the http ``Authentication`` header. The credentials are base64 encoded.
For example:

.. code-block:: bash

    $ echo -n 'user@example.org:password' | base64
    dXNlckBleGFtcGxlLm9yZzpwYXNzd29yZA==

    $ curl -ssl -H 'Authorization: Basic dXNlckBleGFtcGxlLm9yZzpwYXNzd29yZA==' 'https://archive.ucolick.org/archive/data/?obs_date=eq,2019-05-24&results=filename&page_size=5'

.. code-block:: json

    {"count":81,
    "next":"https://archive.ucolick.org/archive/data/?obs_date=eq%2C2019-05-24&page=2&page_size=5&results=filename",
    "previous":null,
    "results":[{"filename":"2019-05/23/APF/ucb-blo212.fits","id":138},
               {"filename":"2019-05/23/APF/ucb-blo223.fits","id":141},
               {"filename":"2019-05/23/APF/ucb-blo241.fits","id":143},
               {"filename":"2019-05/23/APF/ucb-blo197.fits","id":144},
               {"filename":"2019-05/23/APF/ucb-blo227.fits","id":148}]}

Session Based Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Session based authentication requires both login and logout API calls.

:URL:  archive/api/login
:Code: lick_archive/apps/archive_auth

Login is a two-step process. First do a GET request to get the CSRF (Cross-Site Request Forgery) tokens. This will return a
token to include in subsequent API calls and also setup the neccessary session cookies.
If a user is already logged in on, this session the resulting JSON will have ``logged_in`` as true, 
and ``user`` will indicate which username is logged in.

.. code-block:: bash

    $ curl -c cookies.txt -b cookies.txt https://archive.ucolick.org/archive/api/login

.. code-block:: json

    {"logged_in": false, "user": "", "csrfmiddlewaretoken": "aaaaaaaaa"}

Second pass the username and password to the API, using the ``csrfmiddlewaretoken`` from above. Notice that ``Referer`` is used
to help protect against CSRF attacks.

.. code-block:: bash

    $ pw=$(systemd-ask-password)
    curl -c cookies.txt -b cookies.txt -d "username=user@example.org" -d "password=${pw}" -d "csrfmiddlewaretoken=aaaaaaaaa" -H "Referer: https://archive.ucolick.org/login.html" https://archive.ucolick.org/archive/api/login

.. code-block:: json

    {"logged_in": true, "user": "user@example.org", "csrfmiddlewaretoken": "bbbbbbbbb"}


:URL: archive/api/logout
:Code: lick_archive/apps/archive_auth

Logging out works the same as logging in, requiring a CSRF token. The logout can be verified with a GET to the login API.

.. code-block:: bash

    $ curl -c cookies.txt -b cookies.txt https://archive.ucolick.org/archive/api/login

.. code-block:: json

    {"logged_in": true, "user": "user@example.org", "csrfmiddlewaretoken": "ccccccccc"}

.. code-block:: bash

    $ curl -c cookies.txt -b cookies.txt -d "csrfmiddlewaretoken=ccccccccc" -H 'Referer: https://archive.ucolick.org/index.html' https://archive.ucolick.org/archive/api/logout

.. code-block:: bash

    $ curl -c cookies.txt -b cookies.txt https://archive.ucolick.org/archive/api/login
    
.. code-block:: json

    {"logged_in": false, "user": "", "csrfmiddlewaretoken": "ddddddddd"}

