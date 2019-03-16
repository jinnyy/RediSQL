import redis
import re
from Parser import Like, ConvertCond





def ShowTables(conn):
    tblist = []
    for name in list(r.smembers('Tables')):
        tblist.append(name.decode('utf-8'))
    return tblist



# 'create table student (attrName type (,attrName type)*)'
def CreateTable(r, query):
    query = query.lower()
    tbname = query.split(" ")[2].split("(")[0]
    if tbname[-1]=="(":
        tbname = tbname[:-1]
    attrs = query[14+len(tbname):-1].split(",")
    r.sadd('Tables', tbname)
    # hash에 attribute와 type 넣음
    for i in range(len(attrs)):
        # attrName type
        temp = attrs[i].strip()
        
        if temp[0]=="(":
            temp = temp[1:].strip()
        col_info = temp.split(" ")
        r.hset(tbname, col_info[0], col_info[1])
    # value를 담을 list 안에 count에 해당하는 값 push
    r.lpush(tbname+'_rows', 0)


    
def Insert(conn, query):
    #######################################################
    # table이름 찾기
    query = query[:query.find(';')].lower()
    idx = query.find('(')
    inquery = query[query.find('insert into ')+12:]
    tbname = inquery.split(' ')[0].strip() # table name
    
    if tbname not in ShowTables(conn):
        print('Table {} does not exist'.format(tbname))
        return None
    
    # column 부분과 value 부분
    # 괄호 제거
    query = re.sub("\(|\)", "", query)
    qrs = query[idx:].split('values')
    #######################################################
    columns = []
    values = []
    if len(qrs)==2:
        columns = qrs[0].split(",")
        values = qrs[1].split(",")
        for i in range(len(columns)):
            columns[i] = columns[i].strip()
            values[i] = values[i].strip()
    else:
        cols = conn.hkeys(tbname)
        for i in cols:
            columns.append(i.decode('utf8'))
        values = qrs[0].split(",")
        for i in range(len(values)):
            values[i] = values[i].strip()

    # 테이블이름_val이라는 리스트에 개수 update하고 실제로 row에 넣음
    i = 0
    # lpop으로 count값 가져옴
    cnt = conn.lpop(tbname+'_rows').decode('utf8')
    key_name = tbname+'_val_'+str(cnt)
    for col in conn.hkeys(tbname):
        if col.decode('utf-8') in columns:
            conn.hset(key_name, col.decode('utf-8'), values[i])
            i+=1
        else:
            conn.hset(key_name, col.decode('utf-8'), None)
    conn.lpush(tbname+'_rows', key_name)
    # 다 끝나면 개수를 update
    conn.lpush(tbname+'_rows', int(cnt)+1)



def Delete(conn, query):
    tbname = query.split(' ')[2]
    rows = []
    listname = tbname+'_rows'
    length_temp = conn.lpop( listname )
    length = conn.llen( listname )
    
    if ' where' in query.lower():
        rows_temp = Where(conn, query, tbname)
        for i in range(length):
            row_cur = conn.rpop(listname)
            if i < len(rows_temp):
                if row_cur == rows_temp[i]:
                    rows.append(rows_temp[i])
                    i += 1
                else:
                    conn.lpush(listname, row_cur)
            else:
                conn.lpush(listname, row_cur)
    else:
        for i in range(length):
            row_cur = conn.rpop(listname)
            rows.append( row_cur )
    
    # rows에 있는 모든 key 삭제
    # for문으로 rows를 돌면서 각 row 삭제
    for row_key in rows:
        conn.delete( row_key )
    # length를 설정
    try:
        conn.lpush( listname, int(length_temp.decode('utf-8')) - len(rows) )
    except:
        print('Nothing to delete')



def ParseUpdate(query):
    query = query.upper() ##

    inquery = query[query.find('UPDATE ')+7:]
    table_name = inquery.split(' ')[0].split('\n')[0].strip() # table name
    
    setquery = query[query.find('SET')+3:query.find('WHERE')].split(',')
    set_dic = {}
    for i in setquery:
        k,v = i.split('=')
        k = k.strip()
        v = v.strip()
        set_dic[k] = v
    
    where = query[query.find('WHERE'):query.find(';')].strip()
    
    result_dic = {}
    result_dic['table_name'] = table_name
    result_dic['set'] = set_dic
    result_dic['where'] = where
    return result_dic



def Update(conn, query):
    info = ParseUpdate(query)
    rows = Where(conn, " "+info['where'], info['table_name'].lower())
    updated = False
    for row in rows:
        for key in info['set']:
            val = info['set'][key].lower()
            if val[0]=='"':
                val = val[1:]
            if len(val)>1:
                if val[-1]=='"':
                    val = val[:-1]
            conn.hset(row, key.lower(), val)
        updated = True
    if updated:
        print("Succesfully Updated")
    else:
        print("Nothing is Updated")



def DropTable(conn, query):
    tbname = query.split(" ")[2]
    if tbname[-1]==";":
        tbname = tbname[:-1]
        
    if tbname not in ShowTables(conn):
        print('Table {} does not exist'.format(tbname))
        return None   
    
    # 모든 row 삭제
    Delete(r, 'delete from '+tbname)
    # 레디스에서 테이블 메타정보 삭제
    conn.delete(tbname)
    conn.srem('Tables', tbname)
    conn.delete(tbname+"_rows")



## where()함수
## * 조건에 맞는 row의 keyname만 리턴
def Where(conn, query, tbname):
    print("=====================================================")
    listname = tbname+'_rows'
    length = conn.llen(listname)
    query = query.lower()
    
    rows = []
    
    # col, query, like
    idx = query.find('where ')
    if idx==-1:
        idx = query.find('where(')
    parsed_dict = ConvertCond(query[idx+5:],conn,tbname)
    
    # like가 있는지 표시
    has_like = False
    if type(parsed_dict)!=type(None):
        if (len(parsed_dict['like']) != 0):
            has_like = True
    else:
        print("Exception: 파싱된 결과의 타입이 "+type(parsed_dict)+"입니다.")
        return
    
    
    for i in range(length):
        keyname = conn.rpop( listname )
        
        # 변수에 값 넣고 찾음
        for col in parsed_dict['cols']:
            temp = col
            val = conn.hget(keyname, col)
            if type(val)==type(None):
                continue
            globals()['{}'.format(temp)] = val.decode('utf-8')
            
        # eval로 query문 실행
        if(eval(parsed_dict['query'])):
            if has_like:
                # LIKE문 처리
                for conds in parsed_dict['like']:
                    # [0] = 변수명
                    # [1] = 정규식
                    temp = conds[0]
                    # hget으로 tbname, temp
                    globals()['{}'.format(temp)] = conn.hget(keyname, col)
                    pat = eval(conds[1])
                    # match
                    if (bool(pat.match(name.decode('utf-8')))):
                        # True라면
                        # row의 key를 결과에 넣음
                        #rows.append(conn.hgetall(keyname))
                        rows.append(keyname)
            else:
                rows.append(keyname)
        
        conn.lpush( listname, keyname )
    return rows



def printSelect(rows, select_cols,all_cols, tbname, conn):
    results = []
    # for문으로 각 row를 돌면서 각 row 방문
    if select_cols[0].strip() == "*":
        # rows에서 print
        columns=[]
        cols = conn.hkeys(tbname)
        for i in cols:
            columns.append(i.decode('utf8'))
        for i in columns:
            print('{:^10}'.format(i), end=" ")
        print()
        for i in rows:
            for j in columns:
                try:
                    print('{:^10}'.format(i[eval("b'{}'".format(j))].decode('utf8')),end = " ")
                except:
                    print("", end = "")
            print()
    else:
        ##################### [참고] 여기서 count, sum, max, min, avg ##################
        # 일부 column만 가져오는 경우        
        result_dic = {}
        for i in select_cols:
            result_dic[i]=[]
            
        for i in rows:
            for j in select_cols:
                try:
                    val = i[eval("b'{}'".format(j))].decode('utf8')
                    result_dic[j].append(val)
                except:
                    print("", end = "")
            
        for i in all_cols:
            print('{:^10}'.format(i),end=" ")
        print()
        
        notaggre = False
        
        for i in all_cols:
            if 'count(' in i:
                col = i[i.find('count(')+6:i.find(')')]
                print('{:^10}'.format(len(result_dic[col])),end=" ")
            elif 'sum(' in i:
                col = i[i.find('sum(')+4:i.find(')')]
                intlist = map(int,result_dic[col])
                print('{:^10}'.format(sum(intlist)),end=" ")
            elif 'max(' in i:
                col = i[i.find('max(')+4:i.find(')')]
                print('{:^10}'.format(max(result_dic[col])),end=" ")
            elif 'min(' in i:
                col = i[i.find('min(')+4:i.find(')')]
                print('{:^10}'.format(min(result_dic[col])),end=" ")
            elif 'avg(' in i:
                col = i[i.find('avg(')+4:i.find(')')]
                intlist = map(int,result_dic[col])
                print('{:^10}'.format(sum(intlist)/len(result_dic[col])),end=" ")
            else:
                notaggre = True
                
        if(notaggre):
            columns= all_cols
            cols = conn.hkeys(tbname)
            for i in rows:
                for j in columns:
                    try:
                        print('{:^10}'.format(i[eval("b'{}'".format(j))].decode('utf8')),end = " ")
                    except:
                        print("", end = "")
                print()




def Select(conn, query):
    # select_cols, from_tbs
    query_lower = query.lower()
    idx1 = query_lower.find("select ")
    idx2 = query_lower.find("from ")
    idx3 = query_lower.find("where ")
    if idx3==-1:
        idx3 = query_lower.find("where(")
    
    # SELECT 뒷부분
    temp_str = query[idx1+7:idx2].strip()
    origin_all_cols = temp_str.split(",")
    # 쓸 때 각 element들 strip() 한번 더 하기
    
    select_cols = []
    # count, sum, max, min, avg 체크
    all_cols = []
    for i in origin_all_cols:
        all_cols.append(i.strip())
    
    for i in all_cols:
        if 'count(' in i:
            select_cols.append(i[i.find('count(')+6:i.find(')')])
        elif 'sum(' in i:
            select_cols.append(i[i.find('sum(')+4:i.find(')')])
        elif 'max(' in i:
            select_cols.append(i[i.find('max(')+4:i.find(')')])
        elif 'min(' in i:
            select_cols.append(i[i.find('min(')+4:i.find(')')])
        elif 'avg(' in i:
            select_cols.append(i[i.find('avg(')+4:i.find(')')])
        else:
            select_cols.append(i.strip())
                
    select_cols = list(set(select_cols))
    
    # FROM 뒷부분
    tbname = query[idx2+5:].split(" ")[0]  # FROM 뒷부분만 자름
    tbname = tbname.replace(";","")
    # 쓸 때 각 element들 strip() 한번 더 하기
    
    ################### SELECT, FROM 파싱 끝 ##################
    # WHERE문 있는 경우
    rows = []     # rows라는 리스트에 조건에 맞는 row들 넣음
    listname = tbname+'_rows'
    length = conn.llen( listname ) # 실제
    
    # where문이 있는 경우
    if idx3!=-1:
        pick_rows = Where(conn, query, tbname)
        
        for i in pick_rows:
            rows.append(conn.hgetall(i))
        
    # where문이 없는 경우
    else:
        for i in range(length):
            keyname = conn.rpop(listname)
            rows.append(conn.hgetall(keyname))
            # 다시 넣어줌
            conn.lpush( listname, keyname )
    
    #################### WHERE문 처리 끝 ######################
    
    return rows, select_cols, all_cols, tbname, conn

def printSelect(rows, select_cols,all_cols, tbname, conn):
    results = []
    # for문으로 각 row를 돌면서 각 row 방문
    if select_cols[0].strip() == "*":
        # rows에서 print
        columns=[]
        cols = conn.hkeys(tbname)
        for i in cols:
            columns.append(i.decode('utf8'))
        for i in columns:
            print('{:^10}'.format(i), end=" ")
        print()
        for i in rows:
            for j in columns:
                try:
                    print('{:^10}'.format(i[eval("b'{}'".format(j))].decode('utf8')),end = " ")
                except:
                    print("", end = "")
            print()
    else:
        ##################### [참고] 여기서 count, sum, max, min, avg ##################
        # 일부 column만 가져오는 경우        
        result_dic = {}
        for i in select_cols:
            result_dic[i]=[]
            
        for i in rows:
            for j in select_cols:
                try:
                    val = i[eval("b'{}'".format(j))].decode('utf8')
                    result_dic[j].append(val)
                except:
                    print("", end = "")
            
        for i in all_cols:
            print('{:^10}'.format(i),end=" ")
        print()
        
        notaggre = False
        
        for i in all_cols:
            if 'count(' in i:
                col = i[i.find('count(')+6:i.find(')')]
                print('{:^10}'.format(len(result_dic[col])),end=" ")
            elif 'sum(' in i:
                col = i[i.find('sum(')+4:i.find(')')]
                intlist = map(int,result_dic[col])
                print('{:^10}'.format(sum(intlist)),end=" ")
            elif 'max(' in i:
                col = i[i.find('max(')+4:i.find(')')]
                print('{:^10}'.format(max(result_dic[col])),end=" ")
            elif 'min(' in i:
                col = i[i.find('min(')+4:i.find(')')]
                print('{:^10}'.format(min(result_dic[col])),end=" ")
            elif 'avg(' in i:
                col = i[i.find('avg(')+4:i.find(')')]
                intlist = map(int,result_dic[col])
                print('{:^10}'.format(sum(intlist)/len(result_dic[col])),end=" ")
            else:
                notaggre = True
                
        if(notaggre):
            columns= all_cols
            cols = conn.hkeys(tbname)
            for i in rows:
                for j in columns:
                    try:
                        print('{:^10}'.format(i[eval("b'{}'".format(j))].decode('utf8')),end = " ")
                    except:
                        print("", end = "")
                print()




def Groupby(conn,query):
    
    select = query[query.find('select ')+7:query.find('from')].split(",")
    select = [x.strip() for x in select]

    by = query[query.find('group by ')+9:].split(" ")[0]
    having = query[query.find('having ')+7:query.find(';')]
    
    select_query = query[query.find('select'):query.find('group by')].strip()+';'
    print(select_query)
    result_temp, select_cols,all_cols, tbname, conn=Select(conn, select_query)
    
    result_dic = {}
    result_dic['table_name'] = tbname
    result_dic['select'] = select
    result_dic['by'] = by 
    result_dic['having'] = having
    
    by_dic = {}
    for i in result_temp:
        tem = eval("b'{}'".format(by))
        by_value = i[tem].decode('utf8')
        if by_value in by_dic:
            by_dic[by_value].append(i)
        else:
            by_dic[by_value]=[i]
            
    for i in all_cols:
        print('{:^10}'.format(i),end=" ")
    print()
    
    for i in by_dic:
        if by in select:
            by_tem = eval("b'{}'".format(by))
            print(by_dic[i][0][by_tem].decode('utf8'), end = " ")
            
        temp_agg = 0
        
        for x in select:
            if(x!=by):
                
                if 'count(' in x:
                    col = x[x.find('count(')+6:x.find(')')]
                    for j in by_dic[i]:
                        col_tem = eval("b'{}'".format(col))
                        temp_agg+=1
                    print('{:^10}'.format(temp_agg), end = "")
                    
                elif 'sum(' in x:
                    col = x[x.find('sum(')+4:x.find(')')]
                    for j in by_dic[i]:
                        col_tem = eval("b'{}'".format(col))
                        temp_agg+=int(j[col_tem].decode('utf8'))
                    print('{:^10}'.format(temp_agg), end = "")
                    
                elif 'max(' in x:
                    col = x[x.find('max(')+4:x.find(')')]
                    for j in by_dic[i]:
                        col_tem = eval("b'{}'".format(col))
                        aa = int(j[col_tem].decode('utf8'))
                        if(aa>temp_agg):
                            temp_agg = aa
                    print('{:^10}'.format(temp_agg), end = "")
                    
                elif 'min(' in x:
                    temp_agg = 9999999;
                    col = x[x.find('min(')+4:x.find(')')]
                    for j in by_dic[i]:
                        col_tem = eval("b'{}'".format(col))
                        aa = int(j[col_tem].decode('utf8'))
                        if(aa<temp_agg):
                            temp_agg = aa
                    print('{:^10}'.format(temp_agg), end = "")
                    
                elif 'avg(' in x:
                    col = x[x.find('avg(')+4:x.find(')')]
                    count = 0
                    for j in by_dic[i]:
                        col_tem = eval("b'{}'".format(col))
                        temp_agg+=int(j[col_tem].decode('utf8'))
                        count += 1
                    print('{:^10.3f}'.format(temp_agg/count), end = "")                
                
                
        print()
        
    return result_dic
