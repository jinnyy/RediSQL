import re





# Like()
# - LIKE문 처리
# - [예] "LIKE A1.8301" --> "re.compile(r'^A1.8301$')"
def Like(query):
    query = query.replace('"',"'")
    like = query[query.find("LIKE '")+6:]
    like = like[:like.find("';")]
    
    pat = re.compile("%")
    like = pat.sub(".*", like)
    
    pat_underbar = re.compile("_")
    like = pat_underbar.sub(".", like)
    
    re_compile = "re.compile(r'^{}$')".format(like)
    return re_compile




# ConvertCond()
# - WHERE문 뒤에 오는 쿼리문 처리
# - Python에서 사용가능한 문법으로 변환
def ConvertCond(query, conn, tbname):
    query = query.lower().strip()
    col_names = []
    if query[-1]!=";":
        query += ";"
    
    
    ## 1) "<>" --> "!=" ########################################
    pat = re.compile("<>")
    query = pat.sub("!=", query)
    
    
    ## <, > 따로 처리 ##########################################
    ## <
    idxs = [m.start() for m in re.finditer('<', query)]
    for idx in idxs:
        col_name = query[:idx].strip().split(" ")[-1]
        col_names.append(col_name)
        #print(col_name)
        if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
            if idx - len(col_name) <= 0:
                query = query[:idx - len(col_name)] + " int(" + col_name + ")" + query[idx:]
            else:
                query = query[:idx - len(col_name) -1] + " int(" + col_name + ")" + query[idx:]
    ## >
    idxs = [m.start() for m in re.finditer('>', query)]
    for idx in idxs:
        col_name = query[:idx].strip().split(" ")[-1]
        col_names.append(col_name)
        if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
            if idx - len(col_name) <=0:
                query = query[:idx - len(col_name)] + " int(" + col_name + ")" + query[idx:]
            else:
                query = query[:idx - len(col_name) -1] + " int(" + col_name + ")" + query[idx:]
    
    ## 2) "[<|>|]=" ###########################################
    eq_idxs = [m.start() for m in re.finditer('=', query)]
    i=0
    for idx in eq_idxs:
        if not (query[idx-1]=="<" or query[idx-1]==">" or query[idx-1]=="!"):
            query = query[:idx+i] + "=" + query[idx+i:]
            # =를 추가했으니까 원래 찾았던 index보다 한칸 뒤를 봐야 함
            i+=1
        # col_names에 column 이름 넣기
        temp_list = query[:idx+i].strip().split(" ")
        if len(temp_list)<2:
            temp = temp_list[-1]
            if ("<" in temp):
                idx2 = temp.find("<")
                col_names.append(temp[:idx2])
            elif  (">" in temp):
                idx2 = temp.find(">")
                col_names.append(temp[:idx2])
            elif ("!" in temp):
                idx2 = temp.find("!")
                col_name = temp[:idx2]
                col_names.append(col_name)
                # int( ### ) 붙여주기
                if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
                    if idx2 - len(col_name) <=0:
                        query = query[:idx2 - len(col_name)] + " int(" + col_name + ")" + query[idx2:]
                    else:
                        query = query[:idx2 - len(col_name) -1] + " int(" + col_name + ")" + query[idx2:]
            elif ("=" in temp):
                idx2 = temp.find("=")
                col_name = temp[:idx2]
                col_names.append(col_name)
                if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
                    if idx2 - len(col_name) <=0:
                        query = query[:idx2 - len(col_name)] + " int(" + col_name + ")" + query[idx2:]
                    else:
                        query = query[:idx2 - len(col_name) -1] + " int(" + col_name + ")" + query[idx2:]
            else:
                print("Wrong Input. 다시 시도하세요.")
                return
        else:
            if query[idx-1]=="<" or query[idx-1]==">":
                continue
            col_name = temp_list[-2].strip().lower()
            # int( ### ) 붙여주기
            if type(conn.hget(tbname, col_name))==type(None):
                col_name = temp_list[-1][:-1]
            col_names.append(col_name)
            if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
                if query[idx+1]=="=":
                    query = query[:idx - len(col_name) -1] + " int(" + col_name + ")" + query[idx:]
                else:
                    query = query[:idx - len(col_name) -2] + " int(" + col_name + ")" + query[idx:]
    
    
    ## 3) BETWEEN #############################################
    ## "column_name BETWEEN value1 AND value2" --> "value1 < column_name < value2" (대소비교 후)
    idx = query.lower().find(' between ')
    while idx!=-1:
        idx += 1
        # column 이름 찾고 넣기
        col_name = query[:idx].strip().split(" ")[-1]
        col_names.append(col_name)
        
        # and를 찾음
        andidx = query[idx+8:].lower().find(" and ")
        if andidx==-1:
            print("Syntax Error: BETWEEN 후에 AND가 필요합니다.")
            return
        # tempstr: between 뒤의 첫 and의 뒷부분
        tempstr = query[idx+13+andidx:].strip()
        # v1: between 과 and 사이에 있는 값
        v1 = query[idx+8 : idx+8+andidx].strip()
        # v2: and 뒤에 있는 값
        idx2 = tempstr.find(" ")
        if idx2==-1:
            idx2 = tempstr.find(";")
        v2 = tempstr[ : idx2].strip()
        
        # 바꾸기
        newcond = ""
        if v1 < v2:
            ########### if int
            if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
                newcond = "(" + v1 + "<= int(" + col_name + ") and int(" + col_name + ") <=" + v2 + ") "
            else:
                newcond = "(" + v1 + "<=" + col_name + " and " + col_name + "<=" + v2 + ") "
        else:
            ########### if int
            if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
                newcond = "(" + v2 + "<= int(" + col_name + ") and " + col_name + "<=" + v1 + ") " 
            else:
                newcond = "(" + v2 + "<= int(" + col_name + ") and " + col_name + "<=" + v1 + ") "
        
        query = (query[:idx-len(col_name)-1]+ " " + newcond + query[idx+andidx+len(v2)+len(v1)+13:])
        
        # 다음 찾기
        idx = query.lower().find(' between ')
        
            
    ############################################################
    ## 4) LIKE
    idx = query.lower().find(' like ')
    like_res = []
    while (idx != -1):
        # column 이름 찾고 넣기
        temp_list = query[:idx].split(" ")
        if len(temp_list)<=1:
            cond = query[:idx].strip()
        else:
            cond = temp_list[-2]
        col_name = temp_list[-1]
        col_names.append(col_name)
        like_query = query[idx+1:].split(" ")[1]
        if (like_query[-1]==";"):
            like_query = like_query[:-1]
        if (like_query[-1]==")"):
            like_query = like_query[:-1]
        if (like_query[0]=="("):
            like_query = like_query[1:]
        # like 함수 실행
        like_res.append([col_name, Like( "LIKE "+like_query )])
        # like문 제거
        if cond.lower()=="or":
            query = query[:idx-len(col_name)] + "False" + query[idx+len(like_query)+6:]
        else:
            query = query[:idx-len(col_name)] + "True" + query[idx+len(like_query)+6:]
        idx = query.lower().find(' like ')
    
    ## 5) IN ##################################################
    idxs = [m.start() for m in re.finditer(' in ', query)]
    for idx in idxs:
        # column 이름 찾고 넣기
        col_name = query[:idx].split(" ")[-1]
        col_names.append(col_name)
        if ("int" in conn.hget(tbname, col_name).decode('utf-8').lower()):
            query = query[:idx - len(col_name)] + "int(" + col_name + ")" + query[idx:]
    
    ## 리턴
    return {"cols": set(col_names), "query": query[:query.find(';')].strip(), "like": like_res}
