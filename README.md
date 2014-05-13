bfs-gamma-archiver
==================

Little python program that archives gamma radiation data from
Bundesamt für Strahlenschutz (bfs), Germany to a MySQL database.

This is helpful because the bfs provides high-res data only
for a span of 24 hours.

## Quick start

1. [Contact the BFS](http://www.bfs.de/de/bfs/Kontakt) to get a
   personal account for http://odlinfo.bfs.de/daten/

2. On your machine, create a MySQL database and, if necessary, 
   a user with SELECT and INSERT permissions.

3. Copy config.dist.py to config.py

4. Edit config.py to match your MySQL settings and your
   BFS credentials.

5. Optional/recommended: Create a virtual environment and
   activate it.

    virtualenv venv
    source venv/bin/activate

6. Install requirements:

    pip install -r requirements.txt

7. Run the script:

    python download.py -v

