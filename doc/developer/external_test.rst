External Test Procedure
=======================

This test procedure is intended to provide quick validation of the lick archive's functionality
from an external host.

Setup the server
----------------
#. Set your environment::

    $ newgrp mhdata
    $ source /opt/lick_archive/bin/activate

#. If running on a dev vm

    #. Make sure a super user has been created as in :ref:`in the deployment steps <admin_superuser>`.
    #. Make sure the test images exist in the archive, as defined in :file:`/opt/lick_archive/bin/ext_test/ext_test_common.py`.

#. If it does not already exist, create ``test_user`` as a staff user. Set its password, but it should be disabled (Active unchecked if using the :ref:`admin page <archive_admin_page>`).

#. Prepare the ``test_user`` account for testing::

    $ python3 /opt/lick_archive/bin/ext_test/server_init.py

Run the test on a remote system
--------------------------------
#. Clone the git repository if needed::

    git clone https://github.com/UCObservatories/lick_searchable_archive.git

#. Run the tests. Add ``--ssl_ca_bundle=False`` to the commands below if using self-signed certs.

    ::
    
        cd lick_searchable_archive/test/
        pytest test_query.py  --archive_host=archive.ucolick.org
        pytest test_header.py  --archive_host=archive.ucolick.org
        pytest test_download.py --archive_host=archive.ucolick.org

Clean up the server
-------------------
#. Set your environment::

    $ newgrp mhdata
    $ source /opt/lick_archive/bin/activate

#. Disable the ``test_user`` account::

    python3 /opt/lick_archive/bin/ext_test/server_cleanup.py 
