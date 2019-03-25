import numpy as np
import pandas as pd
import sys, traceback

import json
import time
import requests
import math

from datetime import timedelta
from tinydb import TinyDB, Query

url='http://www.omdbapi.com/'
OMDb_API_key = "8dc02a24"

basics = pd.read_csv('basics.csv', index_col=0)
basics = basics[basics['runtimeMinutes'] != '\\N']
# basics = basics.drop(columns=['primaryTitle', 'runtimeMinutes', 'year'])

dbs = []
db_name = 'dbs/filmes_'
db_index = 0
db_ext = '.json'
db = TinyDB(db_name + str(db_index) + db_ext)

while len(db) >= 1000:
  db_index+=1
  db=TinyDB(db_name + str(db_index) + db_ext)

db_list = []

for i in range(db_index+1):
  temp_db = TinyDB(db_name + str(i) + db_ext)
  db_list += list(map(lambda x: x['imdbID'], temp_db.all()))

db_trash = TinyDB('dbs/trash.json')
db_trash_list = list(map(lambda x: x['tt'],db_trash.all()))

basics_left = basics[(~basics.tconst.isin(db_list)) & (~basics.tconst.isin(db_trash_list))]

restante = basics_left.shape[0]
print('\nBAIXADO\t\t[\t' + str(len(db_list)) + '\t]')
print('RESTANTE\t[\t' + str(restante) + '\t]')
print('LIXO\t\t[\t' + str(len(db_trash)) + '\t]')
print('TOTAL\t\t[\t' + str(basics.shape[0]) + '\t]')

it=len(db_list)

not_ok=0

start=time.time()

req=0

tot_db=0
tot_rq=0

print('\n[ Inicializando Download ]')
for tt in basics_left.tconst:
  try:
    if len(db) >= 1000:
      db_index+=1
      db=TinyDB(db_name + str(db_index) + db_ext)

    params = { 'i': tt, 'apikey': OMDb_API_key }
    start_rq = time.time()
    response = requests.get(url, params=params)
    tot_rq = time.time()-start_rq
    req+=1

    if response.json()['Response'] == 'False':
      db_trash.insert({ "tt": tt })
      not_ok+=1
      continue

    if response.status_code == 200 and response.json()['Response'] == 'True':
      start_db = time.time()
      db.insert(response.json())
      tot_db += time.time()-start_db
      it+=1
    else:
      print('\nparou no ' + str(it))
      print(response.text)
      break
  except ValueError as e:
    print('')
    print('\n'+e.doc)
    print('\n>>>>>>>>>>> tconst: ' + tt)
    db_trash.insert({ "tt": tt })
  except KeyboardInterrupt:
    print('\nParando execução...')
    sys.exit()
  except:
    print('')
    traceback.print_exc()
  finally:
    perc = it/(basics.shape[0]-not_ok)
    now = time.time() - start
    print('PROGRESSO: {:6}/{:6}'.format(it, str(basics.shape[0]-not_ok)) + '[ {0:.2f}'.format((perc*100)) + '% ] '
      + str(timedelta(seconds=int(now)))
      + ' ::: [db-delay: {:5}'.format(int(tot_db*1000/it)) + 'ms]'
      + ' ::: [rq-delay: {:5}'.format(int(tot_rq*1000/req)) + 'ms]'
      + ' ::: [dbs: {:3}'.format(db_index+1) + ']'
      + ' ::: [trash: {:6}]'.format(len(db_trash)) , end='\r')

print('\n{}'.format(it))