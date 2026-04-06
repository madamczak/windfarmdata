# Prompt Log

## Entry 1 — 2026-04-03

> I wonder if you can save a prompt to a file in a project directory. For example save this prompt to a .md file and later when I enter a prompt it would be saved as next in that file

## Entry 2 — 2026-04-03 19:59

> ok let's check it - so can you this prompt in prompts.md?

## Entry 3 - 2026-04-03

> ok, cool, you used a script to save a prompt to .md file. Can you do that without this script? Just like initial prompt was saved to a file

## Entry 4 - 2026-04-03

> ok, so now create a file with description of that behavior that each prompt should be saved into a file without external scripts and store it in .md file. I want to use it later as input to default model behavior

## Entry 5 - 2026-04-03

> ok I moved .md files to promptlogger directory

## Entry 6 - 2026-04-03

> create a git repository for this project

## Entry 7 - 2026-04-03

> ok so there is a file that describes default behavior of this model. I don't know it's name but also there is something like skills that are also saved in similiar file. Create this file for me and set it up in correct directory so that my all next prompts take it into account. I want something like this initial list: whatever backend code in python, whatever frontend code in vue js, don't run code in the terminal, create a directory scripts and write scripts in that directory and execute them later as files in terminal

## Entry 8 - 2026-04-03 20:15

> ok add time to prompt logging instructions and add a general rule to not create readme files until asked specifically

## Entry 9 - 2026-04-03 20:30

> ok so there is this idea about and API that would serve data from wind farms. I have 3 sqlite databases for that purpose: Kelmarsh - around 8GB Penmanshiel - around 8gb Hill of Towie - around 30gb  So they are quite big but still able to work comfortably on a PC but in case of cloud computing it could be a limiting factor. At least from perspective of as little cost as possible.  There is this type of file called parquet that is significantly smaller than sqlite db but from API perspective - would it be better to create a storage space in the cloud and store parquet file in comparison to storing sqlite files with indexing on datetime column. Idea of a query is a time based event that was preselected but user would be able to decide how much time before and after the event he/she/it, as we are also talking about scrapping this data in automated manner, would like to select and get access to. Write two files with instructions that would be readable for human and llm model to be implemented. Create instructions directory for that purpose

## Entry 10 - 2026-04-03 20:45

> ok add to general rules that execution of python -c " is forbidden. Every piece of code has to have separate file in scripts directory

## Entry 11 - 2026-04-03 20:50

> ok, there was a mention in C:\Users\adamc\PycharmProjects\windfarmdata\.github\copilot-instructions.md that every prompt should be saved to prompts.md

## Entry 12 - 2026-04-03 21:00

> ok so in this directory C:\Users\adamc\PycharmProjects\data_by_turbine — backup there is data for penmanshiel and kelmarsh. Kelmarsh has 1 file more kelmarsh_status_by_turbine_duration that has fixed duration column. It's calculated based on next event. So create parquet files in data/[wind farm name] directories. When converting to parquet fix the duration column in penmanshiel data. For Kelmarsh create parquet files only from kelmarsh_status_by_turbine_duration and not from kelmarsh_status_by_turbine

## Entry 13 - 2026-04-03 21:10

> run scripts/convert_to_parquet.py

## Entry 14 - 2026-04-03 21:15

> run it execute the command in terminal

## Entry 15 - 2026-04-03 21:20

> ok so data directory contains files with parquet files but i don't know from which database they were created - how do I visualize this data?

## Entry 16 - 2026-04-03 21:25

> ok so rename all the files and include status as prefix to file names

## Entry 17 - 2026-04-03 21:30

> ok cool now transform two files to parquet: C:\Users\adamc\PycharmProjects\data_by_turbine — backup\kelmarsh_data_by_turbine.db and C:\Users\adamc\PycharmProjects\data_by_turbine — backup\penmanshiel_data_by_turbine.db

## Entry 18 - 2026-04-03 21:35

> you didn't record your last prompt in prompts file

## Entry 19 - 2026-04-03 21:40

> Ok i need also hill of towie data farm, I've converted the database earlier. Parquet files for hill of towie wind farm is located in C:\Users\adamc\PycharmProjects\windhilloftowiefarm\data\parquet directory. Copy it to data/hill_of_towie directory

## Entry 20 - 2026-04-03 21:45

> ok, now - to the API part - I want the first endpoint in the API to be the one that displays 3 names of the wind farms Kelmarsh, Penmanshiel and Hill of Towie which are corresponding to directories in data directory

## Entry 21 - 2026-04-03 21:50

> ok, cool, now, commit the changes but exclude data directory

## Entry 22 - 2026-04-03 21:55

> ok, cool, can I have a commit each time I enter a prompt? Just before any change is made or even thought about.

## Entry 23 - 2026-04-03 22:00

> ok, now, make a change to the endpoint that provide names and make it return name and number of turbines in each collection

## Entry 24 - 2026-04-03 22:05

> add if name == main to the api so I could run it from pycharm

## Entry 25 - 2026-04-03 22:10

> ok, it returns {"wind_farms": [{"name": "Kelmarsh", "directory": "kelmarsh", "turbine_count": 6}, {"name": "Penmanshiel", "directory": "penmanshiel", "turbine_count": 15}, {"name": "Hill of Towie", "directory": "hill_of_towie", "turbine_count": 0}], "total": 3} turbine count in hill of towie is 21 as last set have T21_ as prefix - T21_SCTurDigiOut.parquet

## Entry 26 - 2026-04-03 22:15

> ok, so now, next endpoint that comes to my mind is connected to time ranges of each dataset. the earliest and latest datetime for each collection

## Entry 27 - 2026-04-03 22:20

> ok it returns: {"time_ranges": [{"farm": "kelmarsh", "earliest": null, "latest": null, "timestamp_column": null}, {"farm": "penmanshiel", "earliest": null, "latest": null, "timestamp_column": null}, {"farm": "hill_of_towie", "earliest": null, "latest": null, "timestamp_column": null}]} so there is a problem with reading the data. Fix it

## Entry 28 - 2026-04-03 22:25

> ok, commit changes, end of work for today

## Entry 29 - 2026-04-04

> run the server

## Entry 30 - 2026-04-04

> ok, I understand that hill of towie dataset is different so let's focus on kelmarsh and penmanshiel now. I'd like an endpoint that would return a set of column names for kelmarsh and penmanshiel

## Entry 31 - 2026-04-04

> ok now let's add a hill of towie columns to that endpoint

## Entry 32 - 2026-04-04

> ok, now, I want to be able to get data for particular date from those datasets but I want the user to be able to select only columns that are desired to be in that data as there are many of them. So now I want an endpoint that would download data for particular day for particular columns but if no columns are selected I want all of them to be returned

## Entry 33 - 2026-04-04

> ok so it takes quite a long time, perform a request that would check if it works

## Entry 34 - 2026-04-04

> ok, so I think we are ready to produce some initial frontend design. I want a simple website where user could select a wind farm and a day in form of a calendar then the user would need to select column names or specify that all columns have to be returned. Then after clicking a button data would show up on a website in form of a table.

## Entry 35 - 2026-04-04

> ok, I've stopped everything run backend and frontend app

## Entry 36 - 2026-04-04

> ok, looks good but I want to be able to select only dates that have data that I could download. Other dates should be disabled and initial data should be the first available in the selected farm dataset

## Entry 37 - 2026-04-04

> ok, run the backend and frontend app

## Entry 38 - 2026-04-04

> ok looks good but there's an error Cannot detect a timestamp column in C:\Users\adamc\PycharmProjects\windfarmdata\data\kelmarsh\status_turbine_1.parquet

## Entry 39 - 2026-04-04

> ok, cool, but I want to be able to sort and filter the data on the frontend

## Entry 40 - 2026-04-04

> I've discarded previous changes by accident

## Entry 41 - 2026-04-04

> ok so I have an option to select hill of towie wind farm but I get an error: Uncaught (in promise) TypeError: can't access property "length", _ctx.filteredRows is undefined when I select 19.07.2016 and SCTurDigiOut file type

## Entry 42 - 2026-04-04

> ok, cool, I also want a brief report on that data as there are many record with empty data or 0 values across multiple columns so give me a quick report on that above the data table

## Entry 43 - 2026-04-04

> ok so there is this report and it's quite ok but zero values are not good values if they occur so often. Most probably zeros are not expected at all so modify the report to include that point of view

## Entry 44 - 2026-04-04

> ok, now add a button and possibility to download a csv of that data

## Entry 45 - 2026-04-04

> ok, csv is working fine, now I also want a json file to be selected

## Entry 46 - 2026-04-04

> ok, commit changes end of work for today

## Entry 47 - 2026-04-05 00:00

> ok so there are few endpoints already add tests directory and create tests in pytest there

## Entry 48 - 2026-04-05 10:00

> ok, I don't know if there is logging in the endpoint functions, add extensive logging there

## Entry 49 - 2026-04-05 11:00

> ok now as I have tests and I have automated commits I can test every commit I make, cool. Now I want to use that to my advantage and create a initial continous integration system. I want something that works with github the best

## Entry 50 - 2026-04-05 11:30

> there is a problem with remote branch which was added as remote add origin https://github.com/<your-username>/windfarmdata.git change it to https://github.com/madamczak/windfarmdata.git

## Entry 51 - 2026-04-05 12:00

> Change remote branch from https://github.com/<your-username>/windfarmdata.git to https://github.com/madamczak/windfarmdata.git

## Entry 52 - 2026-04-05 13:00

> there is a failure as there is no data on the remote — test_invalid_file_type_returns_400 gets 404 instead of 400 because the directory check fires before file_type validation

## Entry 53 - 2026-04-05 13:30

> ok so I would also add a rule as a ci threshold that all new code should be tested and coverage should not drop more than 1% in each commit

## Entry 54 - 2026-04-05 14:00

> ok, create a terraform script that would create a s3 bucket. I don't have terraform installed just yet, just create the script don't run it

## Entry 55 - 2026-04-05 14:30

> also dockerize this app, skip data directory and create a dockerfile

## Entry 56 - 2026-04-05 15:00

> CORS error: blocked cross-origin request to http://127.0.0.1:8000/wind-farms — fix CORS so app works both in Docker and local dev

## Entry 57 - 2026-04-05 15:30

> ok so now let's add also some performance tests so that there could be another CI threshold that performance should not drop by more than 5% compared to previous build. Of course the first run will set the threshold for now

## Entry 58 - 2026-04-05 16:00

> FAIL Required test coverage of 88% not reached. Total coverage: 64.26% — coverage should not run during performance benchmarks

## Entry 59 - 2026-04-05 16:30

> ok end of work for today


## Entry 52 - 2026-04-05 12:30

> ok, so now every push is made to the repo, tests are being executed, let's test that - add 1 empty line to whatever file and commit and push it

## Entry 60 - 2026-04-06

> so now make Data Quality Report and table with data to be selectable on separate tabs

## Entry 61 - 2026-04-06

> ok, now I want another tab with charts of those data. Use charts js library

## Entry 62 - 2026-04-06

> ok so it works slowly because there are a lot of rows loaded. Add a pagination there and display only 50 rows at the same moment with the possibility to switch forward and backward with the rows

## Entry 63 - 2026-04-06

> ok, cool, it's added to charts tab but add it also to data table part

## Entry 64 - 2026-04-06

> ok switch the order of the tabs - data table should be first, then charts and quality report at the end

## Entry 65 - 2026-04-06

> ok, there is some new code, add tests for it and push everything to github

## Entry 66 - 2026-04-06

> ok so on the charts tab the default displayed charts should be: Wind speed (m/s), Wind direction (°), Nacelle position (°), Power (kW) and Rotor speed, Standard deviation (RPM)

## Entry 67 - 2026-04-06

> ok add also Generator RPM (RPM) to default charts

## Entry 68 - 2026-04-06

> ok, now upload files from kelmarsh directory to r2 cloudflare bucket that has url: https://7cf52f0e0957036ef8b28411ed958be4.r2.cloudflarestorage.com/windfarmdata a directory called kelmarsh is already created

## Entry 69 - 2026-04-06

> ok cool, let's remove hill of towie from frontend for now and connect to the r2 instead of local data directory path

## Entry 70 - 2026-04-06

> ok, I removed kelmarsh and penmanshiel from data directory and now when I run docker compose up --build it does not load anything. Fix it

