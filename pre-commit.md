# Pre-commit

In order to maintain the quality of our code, we have integrated the __pre-commit__ tool into our project.
This ensures that all commits comply with certain standards and requirements.

## Installation

```
pip install -r requirements-dev.txt

pre-commit install
```
To update pre commit run the following commands:

```bash
pre-commit autoupdate
```

## Before every commit

```
pre-commit run --all-files
```
