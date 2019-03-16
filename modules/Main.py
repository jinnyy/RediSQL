import redis
from ExecuteQuery import *
from Parser import *



r = redis.Redis(host='localhost', port=6379, db=0)

while(True):
    query = input('(실행을 종료하려면 "q"를 입력하세요)\nQuery: ').lower()
    if(query == 'q'):
        break;
    else:
        if 'create table' in query:
            CreateTable(r, query)
        elif 'insert' in query:
            Insert(r, query)    
        elif 'delete' in query:
            Delete(r, query)
        elif 'select' in query:
            rows, select_cols,all_cols, tbname, conn=Select(r,query)
            printSelect(rows, select_cols,all_cols, tbname, conn)
        elif 'update' in query:
            Update(r,query)
        elif 'show table' in query:
            print(ShowTables(r))
        elif 'drop table' in query:
            DropTable(r, query)
        elif 'group by' in query:
            Groupby(r, query)s
