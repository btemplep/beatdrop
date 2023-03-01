Development
===========

.. toctree::
   :maxdepth: 1

If you are interested in contributing to beatdrop, this is the place to start!


Setting up the Dev Environment
------------------------------

The official repository can be found on Github at https://github.com/btemplep/beatdrop. 
It should be cloned from there.

After cloning the repo, cd into the root code dir.

Create a virtual environment.

.. code-block:: console

    $ python -m venv venv

.. code-block:: console

    $ source venv/bin/activate

.. code-block:: console

    (venv)$ pip install -U pip

Install the local package in editable mode with the ``all`` and ``dev`` extra dependencies. 

.. code-block:: console

    (venv)$ pip install -e .[dev,all]

If you use pyenv to manage python environments you can install all the recommended python versions with:

.. code-block:: console

    $ pyenv install

Nox
---

Nox can be used for several pieces of automation and to generally help with development. 
You can see what options are available with 

.. code-block:: console

    $ nox --list


pyenv
-----

To run the tests for multiple python environments they must be available.  
I would recommend using `pyenv <https://github.com/pyenv/pyenv>`_ for this. 
The python versions recommend are held in the ``.python-version`` file.
If you use pyenv, you can install the python versions that nox will test with using the following command in the root directory:

.. code-block:: console

    $ pyenv install


Tests
-----

Test code is kept under ``./tests``.  Inside this dir, are other directories to cover separate high-level testing categories.
Currently, ``./tests/unit/`` is the only one, but if there were functional tests they should be put under ``./tests/functional/``, etc.

pytest is used as the test suite for beatdrop. 
It and other plugins are installed with the ``dev`` extra dependency. 

Strive for 100% coverage in all tests.  Try to test as many scenarios and edge cases as possible/feasible.
Any new additions are expected to come with full testing.  
The same is expected for fixed bugs.  There should be tests that cover the scenario in order to avoid regression. 

The unit test can be run from the project root directory with manually with (this saves time over nox):

.. code-block:: console

    (venv)$ pytest -vvv --cov=src/beatdrop --cov-report html tests/unit

This will create a coverage report in ``./htmlcov``.  You can view the results by opening ``./htmlcov/index.html``.


Documentation
-------------

All code that is part of the public API should have docstrings in NumPy format (classes, methods, functions, etc.)
When in doubt, reference other docstrings for similar what it should look like. 

It's also encouraged to write docstrings for the non-public pieces as well. :)

Schedulers and schedule entries should have their own page and be put under their respective toctree.

