# InnoHack - Record Linkage case

# 🔎 About
The recent data digitization created the necessity for linking individuals from one misformatted source to another. We evaluate an automated methods for record linkage using machine learning techniques.

# 🧬 Project Structure
1. ``docker-entrypoint-initdb.d/`` - contains scripts for clickhouse database initialization
2. ``input_data/`` - must contain files that need to be preproccessed and put into the database
3. ``scripts/`` - contains the source code for the project (main.py)
    -  ``toolkit/`` - contains utility functions (tool_functions.py)
4. ``dockerfile`` - a set of instructions used for building a custom docker image
5. ``docker-compose.yaml`` - script used to run multiple docker containers


# ⬇️ Setup
To execute the project you first need to clone the repository and add necessary files to the ```input_data``` folder.
```
cd InnoHack
docker compose up
```
