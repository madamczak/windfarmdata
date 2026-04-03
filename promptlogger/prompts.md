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

