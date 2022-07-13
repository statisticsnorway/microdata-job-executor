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



## Built with
* [Poetry](https://python-poetry.org/) - Python dependency and package management
* [PyMongo](https://pymongo.readthedocs.io/en/stable/) - MongoDB Driver
* [PyArrow](https://arrow.apache.org/docs/python/) - Apache Arrow
* [Pandas](https://pandas.pydata.org/) - Data analysis and manipulation
* [microdata-validator](https://pypi.org/project/microdata-validator/) - dataset validation

