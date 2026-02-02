# microdata-job-executor

Executes work for a datastore by polling a job-service for available jobs. Uses subprocesses for asynchronous workloads.

## Contribute

### Set up

To work on this repository you need to install [uv](https://docs.astral.sh/uv/).
After cloning the repo, install and activate an environment with:
```sh
uv venv && uv sync && source .venv/bin/activate 
```
## Pre-commit
There are currently 3 active rules: Ruff-format, Ruff-lint and sync lock file.
Install pre-commit 
```sh
pip install pre-commit
```
If you've made changes to the pre-commit-config.yaml or its a new project install the hooks with:
```sh
pre-commit install
```
Now it should run when you do:
```sh
git commit
```

By default it only runs against changed files. To force the hooks to run against all files:
```sh
pre-commit run --all-files
```
if you dont have it installed on your system you can use: 
(but then it wont run when you use the git-cli)
```sh
uv run pre-commit
```
Read more about [pre-commit](https://pre-commit.com/#intro)

### Running tests

Open terminal and go to root directory of the project and run:

```
uv run pytest
```

### Build docker image

```
docker buildx build --tag job-executor:test-local .
```

## Built with
- [Uv](https://docs.astral.sh/uv/) - Python dependency and package management
- [PyArrow](https://arrow.apache.org/docs/python/) - Apache Arrow
- [Pandas](https://pandas.pydata.org/) - Data analysis and manipulation
- [microdata-tools](https://pypi.org/project/microdata-tools/) - dataset packaging & validation
