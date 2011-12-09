Library for importing datasets into CKAN using the API.

Quickstart
==========

To get the code::

    hg clone https://github.com/okfn/ckanext-importlib.git

The code also requires installed:
 * importlib dependencies (pip-requirements.txt)
 * ckan
 * ckan dependencies (ckan/pip-requirements.txt)

To install the dependencies into a virtual environment::

    virtualenv pyenv
    pip -E pyenv install -e ../ckanext-importlib
    pip -E pyenv install -e ckan
    pip -E ../pyenv-ckanext-importlib install -r ../ckan/pip-requirements.txt
    pip -E pyenv install -r pip-requirements.txt


Tests
=====

To run the tests:: 

    pip -E pyenv install -e nose
    cd ckanext-importlib
    nosetests --ckan ckanext/importlib/tests/
