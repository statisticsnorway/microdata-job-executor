# microdata-job-executor

Executes work for a datastore by polling a job-service for available jobs. Uses subprocesses for asynchronous workloads.

## Contribute

### Set up

To work on this repository you need to install [uv](https://docs.astral.sh/uv/).
After cloning the repo, install and activate an environment with:
```sh
uv venv && uv sync && source .venv/bin/activate 
```

### Running tests

Open terminal and go to root directory of the project and run:

```
uv run pytest
```

### Build docker image

```
docker build --tag job_executor .
```

### Running with Wiremock

To stub out collaborating services run the following:

```
cd wiremock
docker run -it --rm \
-p 8080:8080 \
--name wiremock \
-v $PWD:/home/wiremock \
wiremock/wiremock:3x
```

Access http://localhost:8080/__admin/mappings to display the mappings.
There is an initial set of mappings under `wiremock/mappings`. Feel free to add more if needed.

Then set the PSEUDONYM_SERVICE_URL and JOB_SERVICE_URL to http://localhost:8080 and run the application.

## Built with
- [Uv](https://docs.astral.sh/uv/) - Python dependency and package management
- [PyArrow](https://arrow.apache.org/docs/python/) - Apache Arrow
- [Pandas](https://pandas.pydata.org/) - Data analysis and manipulation
- [microdata-tools](https://pypi.org/project/microdata-tools/) - dataset packaging & validation
