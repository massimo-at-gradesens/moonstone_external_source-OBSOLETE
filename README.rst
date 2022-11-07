::

  ****************************************************
  GradeSens - Moonstone External Source python package

  Copyright 2022, Gradesens AG
  ****************************************************

Moontone External Source
========================
Moonstone package to retrieve measurement data from external sources.


Build, packaging, testing and deployment
========================================

This package is packaged and maintained with Poetry
(https://python-poetry.org/)

Poetry cheatsheet
-----------------
Create or re-activate a development shell
_________________________________________
::

  poetry shell

Build the project
_________________
::

  poetry build

Install the project in the development shell for local tests
____________________________________________________________
::

  poetry install

Test the project in the development shell
_________________________________________
::

  tox

Install the recommended GIT hooks
---------------------------------
Few recommended GIT hooks are available. It is recommended to installed them
in every new GIT clone, to ease the collaboration among different users (e.g.
to enfore the same coding style, etc.).

To install the recommended GIT hooks:

::

  pre-commit install
  pre-commit install-hooks


