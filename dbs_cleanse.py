from tinydb import TinyDB, Query
import json, csv

db_name='dbs/filmes_'
db_index=0
db_ext='.json'

db = TinyDB(db_name + str(db_index) + db_ext)

err=0
while len(db) > 0:
    db.remove(Query().Type != 'movie')
    db_list = db.all()
    
    it=0
    first=True
    for row in db_list:
        if first:
            keys1=row.keys()
            first=False
        
        keys2=row.keys()

        if keys2 != keys1:
            print('\nERROR ::: db [ {:3} ] ::: row [ {:4} ]'.format(db_index, it))
            
            print(set(keys1).symmetric_difference(set(keys2)))
            err+=1
        else:
            print('db [ {:3} ] ::: row [ {:4} ]'.format(db_index, it), end='\r')
        it+=1


    db_index+=1
    db = TinyDB(db_name + str(db_index) + db_ext)

print('\nerrors {}'.format(err))