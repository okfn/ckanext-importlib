Quickstart
==========

To get the code::

    hg clone http://bitbucket.org/okfn/ckanext-importlib

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
