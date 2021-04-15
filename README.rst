===============
occupationcoder
===============


.. image:: https://img.shields.io/pypi/v/occupationcoder.svg
        :target: https://pypi.python.org/pypi/occupationcoder

.. image:: https://img.shields.io/travis/aeturrell/occupationcoder.svg
        :target: https://travis-ci.com/aeturrell/occupationcoder

.. image:: https://readthedocs.org/projects/occupationcoder/badge/?version=latest
        :target: https://occupationcoder.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status


A tool to assign standard occupational classification codes to job vacancy descriptions
---------------------------------------------------------------------------------------

Given a job title, job description, and job sector the algorithm assigns
a UK 3-digit standard occupational classification (SOC) code to the job.
The algorithm uses the **SOC 2010** standard, more details of which can
be found on `the ONS'
website <https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc/soc2010>`__.

This code originally written by Jyldyz Djumalieva, `Arthur
Turrell <http://aeturrell.github.io/home>`__, David Copple, James
Thurgood, and Bradley Speigner. If you use this code please cite:

Turrell, A., Speigner, B., Djumalieva, J., Copple, D., & Thurgood, J.
(2019). `Transforming Naturally Occurring Text Data Into Economic
Statistics: The Case of Online Job Vacancy
Postings <https://www.nber.org/papers/w25837>`__ (No. w25837). National
Bureau of Economic Research.

::

    @techreport{turrell2019transforming,
      title={Transforming naturally occurring text data into economic statistics: The case of online job vacancy postings},
      author={Turrell, Arthur and Speigner, Bradley and Djumalieva, Jyldyz and Copple, David and Thurgood, James},
      year={2019},
      institution={National Bureau of Economic Research}
    }

* Documentation: https://occupationcoder.readthedocs.io.

Pre-requisites
~~~~~~~~~~~~~~

See `setup.py` for a full list of Python packages.

occupationcoder is built on top of `NLTK <http://www.nltk.org/>`__ and
uses 'Wordnet' (a corpora, number 82 on their list) and the Punkt
Tokenizer Models (number 106 on their list). When the coder is run, it
will expect to find these in their usual directories. If you have nltk
installed, you can get them corpora using ``nltk.download()`` which will
install them in the right directories or you can go to
`http://www.nltk.org/nltk_data/ <http://www.nltk.org/nltk_data/>`__ to
download them manually (and follow the install instructions).

A couple of the other packages, such as
`fuzzywuzzy <https://github.com/seatgeek/fuzzywuzzy>`__ do not come
with the Anaconda distribution of Python. You can install these via pip
(if you have access to the internet) or download the relevant binaries
and install them manually.

File and folder description
~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  ``occupationcoder/coder`` applies SOC codes to job descriptions
-  ``occupationcoder/createdictionaries`` turns the ONS' index of SOC
   code into dictionaries used by ``occupationcoder/coder``
-  ``occupationcoder/dictionaries`` contains the dictionaries used by
   ``occupationcoder/coder``
-  ``occupationcoder/outputs`` is the default output directory
-  ``occupationcoder/testvacancies`` contains 'test' vacancies to run
   the code on
-  ``occupationcoder/utilities`` contains helper functions which mostly
   manipulate strings

Installation via terminal using pip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download the package and navigate to the download directory. Then use

.. code-block:: shell

    python setup.py sdist
    cd dist
    pip install occupationcoder-version.tar.gz

The first line creates the .tar.gz file, the second navigates to the
directory with the packaged code in, and the third line installs the
package. The version number to use will be evident from the name of the
.tar.gz file.

Running the code as a python script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Importing, and creating an instance, of the coder

.. code-block:: python

    import pandas as pd
    from occupationcoder.coder import coder
    myCoder = coder.Coder()

To run the code on a single job, use the following syntax with the
``codejobrow(job_title,job_description,job_sector)`` method:

.. code-block:: python

    if __name__ == '__main__':
        myCoder.codejobrow('Physicist','Calculations of the universe','Professional scientific')

The ``if`` statement is required because the code is parallelised. Note
that you can leave some of the fields blank and the algorithm will still
return a SOC code.

To run the code on a file (eg csv name 'job\_file.csv') with structure

+--------------+-------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+
| job\_title   | job\_description                                                                                                  | job\_sector                                       |
+==============+===================================================================================================================+===================================================+
| Physicist    | Make calculations about the universe, do research, perform experiments and understand the physical environment.   | Professional, scientific & technical activities   |
+--------------+-------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+

use

.. code-block:: python

    df = pd.read_csv('path/to/foo.csv')
    df = myCoder.codedataframe(df)

This will return a new dataframe with SOC code entries appended in a new
column:

+--------------+-------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+-------------+
| job\_title   | job\_description                                                                                                  | job\_sector                                       | SOC\_code   |
+==============+===================================================================================================================+===================================================+=============+
| Physicist    | Make calculations about the universe, do research, perform experiments and understand the physical environment.   | Professional, scientific & technical activities   | 211         |
+--------------+-------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+-------------+

Running the code from the command line
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you have all the relevant packages in requirements.txt, download the
code and navigate to the occupationcoder folder (which contains the
README). Then run

.. code-block:: shell

    python -m occupationcoder.coder.coder path/to/foo.csv

This will create a 'processed\_jobs.csv' file in the outputs/ folder
which has the original text and an extra 'SOC\_code' column with the
assigned SOC codes.

Testing
~~~~~~~

To run the tests in your virtual environment, use

.. code-block:: shell

    python -m unittest

in the top level occupationcoder directory. Look in ``test_occupationcoder.py`` for what is run and examples of use. The output appears in the 'processed\_jobs.csv' file in the outputs/
folder.

Acknowledgements
~~~~~~~~~~~~~~~~

We are very grateful to Emmet Cassidy for testing this algorithm.

Disclaimer
~~~~~~~~~~

This code is provided 'as is'. We would love it if you made it better or
extended it to work for other countries. All views expressed are our
personal views, not those of any employer.


Credits
-------

The development of this package was supported by the Bank of England.

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

