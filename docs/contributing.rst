.. This file was adapted from the pandas documentation.
.. _contributing:

***********************
Contributing to calista
***********************

.. contents:: Table of contents:
   :local:


All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.

.. _contributing.bug_reports:

Bug reports and enhancement requests
====================================

Bug reports and enhancement requests are an important part of making calista more stable and
are curated though Github issues. When reporting an issue or request, please select the `appropriate
category and fill out the issue form fully <https://github.com/Aubay-Data-AI/calista/issues/new/choose>`_
to ensure others and the core development team can fully understand the scope of the issue.

The issue will then show up to the calista community and be open to comments/ideas from others.

.. _contributing.github:

Submitting a pull request
=========================

.. _contributing.version_control:

Version control, Git, and GitHub
--------------------------------

calista is hosted on `GitHub <https://github.com/Aubay-Data-AI/calista>`_, and to
contribute, you will need to sign up for a `free GitHub account
<https://github.com/signup/free>`_. We use `Git <https://git-scm.com/>`_ for
version control to allow many people to work together on the project.

.. If you are new to Git, you can reference some of these resources for learning Git. Feel free to reach out
.. to the :ref:`contributor community <community>` for help if needed:

If you are new to Git, you can reference some of these resources for learning Git:

* `Git documentation <https://git-scm.com/doc>`_.

Also, the project follows a forking workflow further described on this page whereby
contributors fork the repository, make changes and then create a pull request.
So please be sure to read and follow all the instructions in this guide.

If you are new to contributing to projects through forking on GitHub,
take a look at the `GitHub documentation for contributing to projects <https://docs.github.com/en/get-started/quickstart/contributing-to-projects>`_.
GitHub provides a quick tutorial using a test repository that may help you become more familiar
with forking a repository, cloning a fork, creating a feature branch, pushing changes and
making pull requests.

Below are some useful resources for learning more about forking and pull requests on GitHub:

* the `GitHub documentation for forking a repo <https://docs.github.com/en/get-started/quickstart/fork-a-repo>`_.
* the `GitHub documentation for collaborating with pull requests <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests>`_.
* the `GitHub documentation for working with forks <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks>`_.

Getting started with Git
------------------------

`GitHub has instructions <https://docs.github.com/en/get-started/quickstart/set-up-git>`__ for installing git,
setting up your SSH key, and configuring git.  All these steps need to be completed before
you can work seamlessly between your local repository and GitHub.

.. _contributing.forking:

Create a fork of calista
------------------------

You will need your own copy of calista (aka fork) to work on the code. Go to the `calista project
page <https://github.com/Aubay-Data-AI/calista>`_ and hit the ``Fork`` button. Please uncheck the box to copy only the main branch before selecting ``Create Fork``.
You will want to clone your fork to your machine

.. code-block:: shell

    git clone https://github.com/your-user-name/calista.git calista-yourname
    cd calista-yourname
    git remote add upstream https://github.com/Aubay-Data-AI/calista
    git fetch upstream

This creates the directory ``calista-yourname`` and connects your repository to
the upstream (main project) *calista* repository.

Creating a feature branch
-------------------------

Your local ``main`` branch should always reflect the current state of calista repository.
First ensure it's up-to-date with the main calista repository.

.. code-block:: shell

    git checkout main
    git pull upstream main --ff-only

Then, create a feature branch for making your changes. For example

.. code-block:: shell

    git checkout -b shiny-new-feature

This changes your working branch from ``main`` to the ``shiny-new-feature`` branch.  Keep any
changes in this branch specific to one bug or feature so it is clear
what the branch brings to calista. You can have many feature branches
and switch in between them using the ``git checkout`` command.

.. _contributing.commit-code:

Making code changes
-------------------

.. Before modifying any code, ensure you follow the :ref:`contributing environment <contributing_environment>`
.. guidelines to set up an appropriate development environment.

Then once you have made code changes, you can see all the changes you've currently made by running.

.. code-block:: shell

    git status

For files you intended to modify or add, run.

.. code-block:: shell

    git add path/to/file-to-be-added-or-changed.py

Running ``git status`` again should display

.. code-block:: shell

    On branch shiny-new-feature

         modified:   /relative/path/to/file-to-be-added-or-changed.py


Finally, commit your changes to your local repository with an explanatory commit
message

.. code-block:: shell

    git commit -m "your commit message goes here"

.. _contributing.push-code:

Pushing your changes
--------------------

When you want your changes to appear publicly on your GitHub page, push your
forked feature branch's commits

.. code-block:: shell

    git push origin shiny-new-feature

Here ``origin`` is the default name given to your remote repository on GitHub.
You can see the remote repositories

.. code-block:: shell

    git remote -v

If you added the upstream repository as described above you will see something
like

.. code-block:: shell

    origin  git@github.com:yourname/calista.git (fetch)
    origin  git@github.com:yourname/calista.git (push)
    upstream        git://github.com/Aubay-Data-AI/calista.git (fetch)
    upstream        git://github.com/Aubay-Data-AI/calista.git (push)

Now your code is on GitHub, but it is not yet a part of the calista project. For that to
happen, a pull request needs to be submitted on GitHub.

Making a pull request
---------------------

If everything looks good, you are ready to make a pull request. A pull request is how
code from your local repository becomes available to the GitHub community to review
and merged into project to appear the in the next release. To submit a pull request:

#. Navigate to your repository on GitHub
#. Click on the ``Compare & pull request`` button
#. You can then click on ``Commits`` and ``Files Changed`` to make sure everything looks
   okay one last time
#. Write a descriptive title
#. Write a description of your changes in the ``Preview Discussion`` tab
#. Click ``Send Pull Request``.

This request then goes to the repository maintainers, and they will review
the code.

.. _contributing.update-pr:

Updating your pull request
--------------------------

.. Based on the review you get on your pull request, you will probably need to make
.. some changes to the code. You can follow the :ref:`code committing steps <contributing.commit-code>`
.. again to address any feedback and update your pull request.

Based on the review you get on your pull request, you will probably need to make
some changes to the code.

It is also important that updates in the calista ``main`` branch are reflected in your pull request.
To update your feature branch with changes in the calista ``main`` branch, run:

.. code-block:: shell

    git checkout shiny-new-feature
    git fetch upstream
    git merge upstream/main

If there are no conflicts (or they could be fixed automatically), a file with a
default commit message will open, and you can simply save and quit this file.

If there are merge conflicts, you need to solve those conflicts. See for
example at https://help.github.com/articles/resolving-a-merge-conflict-using-the-command-line/
for an explanation on how to do this.

Once the conflicts are resolved, run:

#. ``git add -u`` to stage any files you've updated;
#. ``git commit`` to finish the merge.

.. note::

    If you have uncommitted changes at the moment you want to update the branch with
    ``main``, you will need to ``stash`` them prior to updating (see the
    `stash docs <https://git-scm.com/book/en/v2/Git-Tools-Stashing-and-Cleaning>`__).
    This will effectively store your changes and they can be reapplied after updating.

After the feature branch has been update locally, you can now update your pull
request by pushing to the branch on GitHub:

.. code-block:: shell

    git push origin shiny-new-feature

Any ``git push`` will automatically update your pull request with your branch's changes
and restart the Continuous Integration checks.

.. _contributing.update-dev:

Updating the development environment
------------------------------------

It is important to periodically update your local ``main`` branch with updates from the calista ``main``
branch and update your development environment to reflect any changes to the various packages that
are used during development.

.. If using :ref:`pip <contributing.pip>` , do:

If using ``pip``, do:

.. code-block:: shell

    git checkout main
    git fetch upstream
    git merge upstream/main
    # activate the virtual environment based on your platform
    python -m pip install --upgrade -r requirements-dev.txt
