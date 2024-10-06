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
- Use mocked S3 using Moto (https://github.com/getmoto/moto)
- Apply modern best practices for Python
- Add simple CI for verifying these practices and tests
- Simple docker file for deployment

## Ref server API:

as in ref_server.py
