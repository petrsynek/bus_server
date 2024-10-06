# Requirements

We want to create an application to process data from an external HTTP server on user request and save them to S3 for further processing & serving. Processing will begin with the user calling the

`POST /process-request?date=<date>` API which will register a task to gather data from the reference server for all available cities on the selected date and upload them to s3 (dont solve edge cases, no registering that the data were dowloaded)

 Afterward, the user can call the

 `GET /country-stats?from=<date>&to<date>` API that will read the data from S3 and return statistics per country and day, how many busses started, what is the total amount of passengers, if there was an accident that day, and what was the average delay.
 - parse the data from file and compute statistics

 When implementing these APIs let's imagine that they should be used "infrequently" (no-caching) and the amount of data per file should be >>100 MB but still procassable in memory.

 Also, we know that this application should be running in production for several years.
 - should have fixed versions

## Additional notes:
- [x] Use a git repository hosted on GitHub (please add me there as a collaborator https://github.com/H00N24)
- [x] Use mocked S3 using Moto (https://github.com/getmoto/moto) - used in tests
- [x] Apply modern best practices for Python ???
- [x] Add simple CI for verifying these practices and tests
- [x] Simple docker file for deployment

## Ref server API:

as in ref_server.py

# Solution

## Architecture

The application is divided into two main components: the API and the data processing.

The API is implemented using litestar. Swagger is used for API documentation.

It can be set up to run local storage or with a mocked S3 bucket.

The data processing is implemented using pandas. It reads the data from the S3 bucket, processes it, and returns the statistics.

The project is covered with tests using pytest.

Project contains Dockerfile and can be run with docker-compose.

**Check makefile for more commands.**

## Possible improvements

- [ ] Use some more robust queue system for processing tasks (Celery, Redis...)
- [ ] Add integration tests (partially done - could be run by docker-compose)
- [ ] Make the RootModel work to display the swagger documentation correctly



