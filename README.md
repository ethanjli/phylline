# phylline
Phylline is a sans-io bidirectional data pipeline framework for making layered communication protocols in Python.

The key idea is a [uniform pipe-and-filter](https://github.com/lawgives/encyclopedia-of-architecture/wiki/Style:-Uniform-Pipe-and-Filter) style for applying data transformations in two directions (send downwards and receive upwards) to create a bidirectional data pipeline. Data pipelines may be linear or branching.


## Unit Tests

Unit tests can be run with the `pytest` command from the repository directory.
This will require installation of the packages listed in `tests/requirements.txt`.
Then you can take advantage of the following features:

- Show test errors immediately, rather than at the end of testing.
- Generate an HTML report at `testreport.html`
- Run tests in parallel with the `-n NUM` argument.
- Run tests related to modified files (as determined by Git) before all other tests with the `--picked=first` argument, and run only tests related to modified files with the `--picked`. By default, "modified files" is releative to unstaged files, but pytest will reinterpret it as relative to the base of the current branch with `--mode=branch`.
- Summarize gaps in test coverage with the `--cov=phylline --cov-branch --cov-report term-missing:skip-covered` arguments; add the `--cov-report html` argument to also generate an html report at `htmlcov/index.html`. Note that if you use these when tests are run in parallel, you may get a warning that no coverage was collected.
- Show a summary of Hypothesis fuzz tests with the `--hypothesis-show-statistics` argument.
- Perform performance profiling with `--profile --profile-svg`. The report is generated to `prof/combined.svg`.

as well as the following built-in features which come with pytest:

- List each individual test with `--verbose`. Note that output is cleaner when tests are *not* run in parallel.
- Only run previously failed tests with `--last-failed`.
- Run previously failed tests before other tests with `--failed-first`.
- Switch to pdb on the first failure with `--pdb  -x`
- Report how long each test took to run with `--durations=0`

For example, to run all tests quickly, prioritizing changed files and previously failed tests, use either of the two following equivalent commands:
```
pytest --picked=first --failed-first -n auto
python3 setup.py test_quick
```
For example, to run only tests for files with modifications since the base of the current Git branch,
prioritizing changed files, use either of the two following equivalent commands:
```
pytest --picked --mode=branch --failed-first -n auto
python3 setup.py test_quick_branch
```
For example, to switch to pdb on the first failure, use either of the two following equivalent commands:
```
pytest --picked=first --failed-first --pdb -x
python3 setup.py test_debug
```
For example, to do performance profiling, use either of the two following equivalent commands:
```
pytest --profile --profile-svg
python3 setup.py test_perf
```
For example, to get a coverage summary, use either of the two following equivalent commands:
```
pytest --cov=phylline --cov-branch --cov-report term-missing:skip-covered --cov-report html
python3 setup.py test_coverage
```
The most complete report can be obtained with either of the two following equivalent commands:
```
pytest --durations=0 --cov=phylline --cov-branch --cov-report term-missing --cov-report html --hypothesis-show-statistics --verbose
python3 setup.py test_complete
```
