Installation
============

Install the base package with pip from pypi:

.. code-block:: console

    $ pip install beatdrop


For particular scheduler storage and  task backends, you will also need to install their extra dependencies.

.. code-block:: console

    $ pip install beatdrop[redis]


Extra dependencies for task backends:

- ``celery`` 

Extra dependencies for scheduler storage:

- ``redis``

- ``sql``

``all`` will install extra dependencies for all task backends and scheduler storage.

.. code-block:: console

    $ pip install beatdrop[all]


``dev`` will install all development dependencies (testing, docs etc.)

.. code-block:: console

    $ pip install beatdrop[dev]