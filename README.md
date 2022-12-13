# microdata-job-executor
Executes work for a datastore by polling a job-service for available jobs. Uses subprocesses for asynchronous workloads.


## Contribute


### Set up
To work on this repository you need to install [poetry](https://python-poetry.org/docs/):
```
# macOS / linux / BashOnWindows
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

# Windows powershell
(Invoke-WebRequest -Uri https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py -UseBasicParsing).Content | python -
```
Then install the virtual environment from the root directory:
```
poetry install
```

#### Intellij IDEA
Use plugin Poetry and add Python Interpreter "Poetry Environment". See https://plugins.jetbrains.com/plugin/14307-poetry

### Running tests
Open terminal and go to root directory of the project and run:
````
poetry run pytest --cov=job_executor/
````

### Build docker image
````
docker build --tag job_executor .
````

### Running with Wiremock
To stub out collaborating services run the following:
````
cd wiremock
docker run -it --rm \
-p 8080:8080 \
--name wiremock \
-v $PWD:/home/wiremock \
wiremock/wiremock:2.33.2
````
Access http://localhost:8080/__admin/mappings to display the mappings.
There is an initial set of mappings under `wiremock/mappings`. Feel free to add more if needed.

Then set the PSEUDONYM_SERVICE_URL and JOB_SERVICE_URL to http://localhost:8080 and run the application.



## Built with
* [Poetry](https://python-poetry.org/) - Python dependency and package management
* [PyMongo](https://pymongo.readthedocs.io/en/stable/) - MongoDB Driver
* [PyArrow](https://arrow.apache.org/docs/python/) - Apache Arrow
* [Pandas](https://pandas.pydata.org/) - Data analysis and manipulation
* [microdata-validator](https://pypi.org/project/microdata-validator/) - dataset validation

