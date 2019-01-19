import pandas as pd
import json
from sqlalchemy import create_engine
import os
from functools import reduce
import numpy as np
import sqlite3

def get_directory_structure(rootdir):
    dir = {}
    rootdir = rootdir.rstrip(os.sep)
    start = rootdir.rfind(os.sep) + 1
    for path, dirs, files in os.walk(rootdir):
        folders = path[start:].split(os.sep)
        subdir = dict.fromkeys(files)
        parent = reduce(dict.get, folders[:-1], dir)
        parent[folders[-1]] = subdir
    return dir

data_directory = "/home/psicktrick/PycharmProjects/Content_seeding/venv/" + "src/sqlite_test/"
image_folder_name =  "to be added"
sqlite_file = data_directory + "production.db"
index_file = "database - database index6.csv"

folder_dict = get_directory_structure(data_directory + image_folder_name)[image_folder_name]

dic = {}
for i in list(folder_dict):
    dic[i] = list(folder_dict[i])

df = pd.DataFrame.from_dict(dic, orient='index')
no_of_columns = len(list(df))
df['listed'] = df.apply(lambda _:'', axis = 1)

for i in range(len(df)):
    df.iloc[i,no_of_columns] = list(df.iloc[i,:(no_of_columns-1)])

df = df['listed']
df = df.reset_index().rename(columns = {"index":"folder_name"})

d = pd.read_csv(data_directory + index_file)
d.rename(columns = {'Search term':'folder_name'}, inplace = True)
d = d[['folder_name', 'tags', 'Account name', 'user_id', 'caption_needed\n']]
d.set_index('folder_name', inplace=True)

df.set_index('folder_name', inplace=True)
df = df.merge(d, left_index=True, right_index=True, how='outer')
df.reset_index(inplace=True)
df = df.set_index(['folder_name', 'tags', 'Account name','user_id', 'caption_needed\n'])['listed'].apply(pd.Series).stack()
df = df.reset_index()
df.columns = ['folder_name', 'tags', 'Account name','user_id', 'caption_needed\n','listed', 'image_name']
df = df.drop('listed', axis=1)
df['Status'] = False
df['path'] = "Scraped images2"+'/'+df['folder_name']+'/'+df['image_name']
df = df.reset_index().rename(columns={'index':'id'})
df['title'] = df.apply(lambda _:"", axis=1)
df['post_name'] = df.apply(lambda _:"", axis=1)
df = df[['id','folder_name','image_name','path','tags','user_id','Status','post_name','title','caption_needed\n','Account name']]

conn = sqlite3.connect(sqlite_file)
odf = pd.read_sql_query("SELECT * FROM seed", conn)
old_images = list(set(list(odf['path'])))
conn.close()

print(df.head())
print(len(df))
df = df[~(df['path'].isin(old_images))]
print(len(df))

engine = create_engine('sqlite:///' + sqlite_file, echo=False)
df.to_sql('seed', con=engine, if_exists='append')
