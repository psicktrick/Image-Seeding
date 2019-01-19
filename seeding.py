import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlite3
from UploadImage import upload_image as u
import json
import os
import schedule
import time
import crontab
import time
from ast import literal_eval

sqlite_file = os.environ['SQLLITE_LOC']
conn = sqlite3.connect(sqlite_file)
df = pd.read_sql_query("SELECT * FROM seed", conn)
folders = list(set(list(df['folder_name'])))
conn.close()

posts = 0
for i in range(2):
    n = 0
    for folder in folders:
        try:
            conn = sqlite3.connect(sqlite_file)
            c = conn.cursor()
            tupl = c.execute("SELECT * FROM seed WHERE folder_name = {} AND status=0 LIMIT 1". \
                             format(json.dumps(folder))).fetchall()
            id_u = tupl[0][1]
            image = tupl[0][3]
            uid = tupl[0][6]
            tags = json.loads(tupl[0][5])
            path = "/home/psicktrick/PycharmProjects/Content_seeding/venv/src/" + tupl[0][4]
            ob = u.Uploader(path, uid, tags)
            image_name, status = ob.send_image()
            if status:
                posts += 1
                c.execute("UPDATE seed SET status = 1 WHERE id = {} AND image_name = {}".format(id_u, json.dumps(image)))
                c.execute("UPDATE seed SET post_name = {} WHERE id = {} AND image_name = {}".format(json.dumps(image_name), id_u, json.dumps(image)))
            print(i, n, folder)
            n += 1
            conn.commit()
            conn.close()
        except IndexError:
            print(i, n, folder, 'Folder exhauseted')
            n += 1
            continue
        except FileNotFoundError:
            print(i, n, folder, 'Image not found')
            n += 1
            continue

try:
    conn.commit()
    conn.close()
    print("Database closed")
except sqlite3.ProgrammingError:
    print("Database closed")

print(str(posts) + " images were posted")
