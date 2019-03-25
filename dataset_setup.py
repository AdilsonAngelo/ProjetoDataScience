from tinydb import TinyDB, Query
from datetime import timedelta
import json, csv, time
import pandas as pd

db_name='dbs/filmes_'
db_index=1
db_ext='.json'

db = TinyDB(db_name + str(db_index) + db_ext)

fieldnames = list(db.all()[0].keys())
fieldnames.remove('Ratings')
fieldnames.remove('Response')
fieldnames += ['tomatoes']
start = time.time()
with open('movies.csv', mode='w') as dataset:
    writer = csv.DictWriter(dataset, fieldnames=fieldnames)
    writer.writeheader()
    while len(db) > 0:
        r=1
        for row in db.all():
            print('db[ {:3} ] ::: row[ {:4} ]'.format(db_index, r), end='\r')
            r+=1

            ''' TRATANDO RATINGS '''
            row['tomatoes'] = ''
            for rating in row['Ratings']:
                if rating['Source'] == 'Rotten Tomatoes':
                    row['tomatoes'] = int(rating['Value'][:-1])
            del row['Ratings']

            ''' TRATANDO RUNTIME '''
            runtime = row['Runtime'].replace('h', '').replace('min', '')
            hours, minutes = (0, 0)

            try:
                if 'h' in row['Runtime'] and 'min' not in row['Runtime']:
                    runtime = float(runtime) * 60
                elif 'h' not in row['Runtime'] and 'min' in row['Runtime']:
                    runtime = float(runtime)
                elif 'h' in row['Runtime'] and 'min' in row['Runtime']:
                    hours, minutes = runtime.split()
                    runtime = float(hours) * 60 + float(minutes)
                else:
                    runtime = 'N/A'

                row['Runtime'] = runtime
            except:
                continue

            del row['Response']

            writer.writerow(row)


        db_index+=1
        db = TinyDB(db_name + str(db_index) + db_ext)
        # if db_index > 30:
        #     break
    print('')

duration = time.time() - start
print('Programa finalizado em {}\n{:3} bd\'s escaneados'.format(timedelta(seconds=int(duration)), db_index))