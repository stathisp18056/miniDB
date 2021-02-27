import database as DB
import datetime


def AnalyseCommand(str,strings):

#ΠΕΡΝΕΙ ΩΣ ΟΡΙΣΜΑΤΑ ΕΝΑ STRING ΤΟ str ΚΑΙ ΜΙΑ ΛΙΣΤΑ ΜΕ strings ΚΑΙ
#ΒΡΙΣΚΕΙ ΑΝ ΟΙ ΛΕΞΕΙΣ ΤΗΣ ΛΙΣΤΑΣ strings ΒΡΗΣΚΟΝΤΕ ΣΤΟ str ΜΕ ΤΗΝ ΣΩΣΤΗ ΣΕΙΡΑ
#ΑΝ ΚΑΤΙ ΤΕΤΟΙΟ ΙΣΧΙΕΙ ΕΠΙΣΤΡΕΦΕΙ True ΚΑΙ ΜΙΑ ΛΙΣΤΑ ΜΕ ΟΤΙ ΕΙΝΑΙ ΓΡΑΜΜΕΝΟ ΕΝΔΙΑΜΕΣΑ ΑΠΟ ΤΙΣ
#ΛΕΞΕΙΣ ΑΥΤΕΣ ΣΤΗΝ ΜΕΤΑΒΛΗΤΗ str και την λιστα strings

    flag=True
    count=0
    tmpstr=str
    values=[]
    while str[0]==" " :
        str=str[1:]
    while str[len(str)-1]==" " :
        str=str[:len(str)-1]
    values.append(str[:str.find(strings[0])])
    for i in range(len(strings)-1):
        values.append(str[str.find(strings[i])+len(strings[i]):str.find(strings[i+1])])
    values.append(str[str.find(strings[len(strings)-1])+len(strings[len(strings)-1]):len(str)])
    for str1 in strings:
        if str.find(str1) < 0 :
            flag = False
            break
    if(flag):
        #print(tmpstr+":    correct")
        #print(SearchedWordsPositions)
        #print(values)
        return[True,values,strings]
    else :
        #print(tmpstr+":    not correct")
        return[False,[],strings]


def select_from(values,table):

#ΠΕΡΝΕΙ ΕΝΣ string ΜΕ values ΚΙ ΑΝ ΕΙΝΑΙ ΣΩΣΤΑ ΓΡΑΜΜΕΝΟ ΣΤΗΝ SQL
#ΕΠΙΣΤΡΕΦΕΙ TRUE TO ONOMA TOY PINAKA KAI ΤΗΝ ΛΙΣΤΑ ΜΕ ΤΙΣ ΜΕΤΑΒΛΙΤΕΣ
#ΠΟΥ ΒΡΙΣΚΟΝΤΕ ΣΤΟ string values

    values = values.split(",")
    tmp = []
    for item in values:
        if item.replace(" ","") != "":
            tmp.append(item.replace(" ",""))
    if tmp == []:
        flag = False
    else:
        flag = True
    return flag,tmp,table

def select_or_delete_from_where(values,table,condition,action):

#ΕΠΙΣΤΡΕΦΕΙ ΤΟ ΟΝΟΜΑ ΤΟΥ ΠΙΝΑΚΑ table ΚΑΙ ΜΙΑ ΛΙΣΤΑ ΜΕΤΑΒΛΗΤΩΝ ΠΟΥ ΠΡΟΚΥΠΤΟΥΝ ΑΠΟ ΤΗΝ
#values ΚΑΙ ΤΟ action ΠΟΥ ΕΙΝΑΙ delete Η select

    values = values.split(",")
    tmp = []
    for item in values:
        if item.replace(" ","") != "":
            tmp.append(item.replace(" ",""))
    if tmp == []:
        flag = False
    else:
        flag = True
    return flag,tmp,table,condition,action

def updete(values,table):

# ΕΠΙΣΤΡΕΦΕΙ ΤΟΝ ΠΙΝΑΚΑ  ΤΙΣ ΜΕΤΑΒΛΗΤΕΣ ΠΟΥ ΠΡΟΚΙΤΕ ΝΑ ΕΝΗΜΕΡΩΘΟΥΝ ΚΑΙ ΤΙΣ ΝΕΕΣ ΤΙΜΕΣ ΤΟΥΣ

    values = values.split(",")
    tmpvalues = []
    for item in values:
        if item.replace(" ","") !="":
            tmpvalues.append(item)
    columns =[]
    newvalues = []
    flag = True
    for item in tmpvalues:
        if len(item.split("=") )!=2:
            flag = False
            break
    if flag:
        for item in tmpvalues:
            columns.append(item.split("=")[0].replace(" ",""))
            newvalues.append(item.split("=")[1].replace(" ",""))
    if tmpvalues == []:
        flag = False
    return flag,columns , newvalues,table

def updete_where(values,table,condition):

# ΕΠΙΣΤΡΕΦΕΙ ΤΟΝ ΠΙΝΑΚΑ  ΤΙΣ ΜΕΤΑΒΛΗΤΕΣ ΠΟΥ ΠΡΟΚΙΤΕ ΝΑ ΕΝΗΜΕΡΩΘΟΥΝ ΚΑΙ ΝΕΕΣ ΤΙΜΕΣ ΤΟΥΣ ΚΑΙ ΤΗΝ ΣΥΝΘΗΚΗ ΓΙΑ ΤΗΝ ΕΝΗΜΕΡΩΣΗ ΤΟΥΣ

    values = values.split(",")
    tmpvalues = []
    for item in values:
        if item.replace(" ","") !="":
            tmpvalues.append(item)
    columns =[]
    newvalues = []
    flag = True
    for item in tmpvalues:
        if len(item.split("=") )!=2:
            flag = False
            break
    if flag:
        for item in tmpvalues:
            columns.append(item.split("=")[0].replace(" ",""))
            newvalues.append(item.split("=")[1].replace(" ",""))
    if tmpvalues == []:
        flag = False
    return flag,columns , newvalues,table,condition

def insert_into_values(columns,values,table):
    flag = True
    if columns.replace(" ","") != "":
        columns = columns.replace(" ", "")[1:len(columns.replace(" ", "")) - 1]
    if values.replace(" ","") != "":
        values=values.replace(" ","")[1:len(values.replace(" ",""))-1]
    else:
        flag = False
    columns = columns.split(",")
    tmpcolumns = []
    for item in columns:
        if item != "":
            tmpcolumns.append(item)
    values = values.split(",")
    if tmpcolumns != [] and len(tmpcolumns) != len(values):
        flag = False
    return flag,tmpcolumns,values,table


def create_table(table):

#ΠΕΡΝΕΙ ΩΣ ΟΡΙΣΜΑΤΑ ΚΟΜΜΑΤΙΑ ΚΩΔΙΚΑ SQL ΚΑΙ ΕΠΙΣΤΡΕΦΕΙ ΤΑ ΟΡΙΣΜΑΤΑ ΠΟΥ ΧΡΗΑΖΕΤΕ Η ΜΕΘΟΔΟΣ create_table() ΤΗΣ database

    flag = True
    while table[0] == " ":
        table = table[1:]
    while table[len(table)-1] == " ":
        table = table[:len(table)-1]

    tablename=""
    columns=[]
    column_names=[]
    column_types=[]
    primary_key=[]
    count = 0
    if table.find("(")<0:
        flag = False
    for char in table:
        if char == "(":
            count+=1
        if char == ")":
            count-=1
        if count<0:
            flag = False
    if count >0:
        flag = False
    try:
        for column in columns:
            column_names.append(column.split(" ")[0])
            column_types.append(column.split(" ")[1])
    except:
        flag = False

    if flag:
        tablename= table[:table.find("(")].replace(" ","")
        columns = list(table[table.find("(")+1:len(table)-1].split(","))
        try:
            for i in range(len(columns)):
                while columns[i][0] ==" ":
                    tmp=columns[i]
                    columns[i] = tmp[1:]
            for column in columns:
                column_names.append(column.split(" ")[0])
                column_types.append(column.split(" ")[1])
        except:
            flag = False
        if columns[len(columns)-1].upper().find("PRIMARY")>-1 and columns[len(columns)-1].upper().find("KEY")>-1:
            try:
                primary_key = list(columns[len(columns)-1][columns[len(columns)-1].find("(")+1:columns[len(columns)-1].find(")")].split(","))
                columns.pop(len(columns)-1)
                column_names.pop(len(column_names)-1)
                column_types.pop(len(column_types)-1)
            except:
                primary_key = column_names
        else:
            primary_key = column_names
        for i in range(len(column_types)):
            if column_types[i].upper().find("STRING")>-1 or column_types[i].upper().find("VARCHAR")>-1:
                column_types[i] = "str"
        types=[]
        for i in column_types:
            if i.upper() == "INT":
                types.append(int)
            elif i.upper() =="CHAR":
                types.append(chr)
            elif i.upper() == "FLOAT":
                types.append(float)
            elif i.upper() == "DATE" or i.upper() == "DATETIME":
                types.append(datetime)
            else:
                types.append(str)


    return flag , tablename , column_names , types , primary_key


def execure_command(command,db):

# ΠΕΡΝΕΙ ΟΡΙΣΜΑΤΑ ΕΝΑ string ΠΟΥ ΕΙΝΑΙ ΕΝΤΟΛΗ SQL ΚΑΙ ΤΗΝ ΒΑΣΗ ΔΕΔΟΜΕΝΩΝ ΠΟΥ ΘΑ ΕΚΤΕΛΕΣΕΙ ΤΗΝ ΕΝΤΟΛΗ

    if AnalyseCommand(command, ["select", "from"])[0]:
        tmp = select_from(AnalyseCommand(command, ["select", "from"])[1][1],AnalyseCommand(command, ["select", "from"])[1][2])
        if tmp[0]:
            db.select(tmp[2],tmp[1])
    elif AnalyseCommand(command, ["select", "from", "where"])[0]:
        tmp = select_or_delete_from_where(AnalyseCommand(command, ["select", "from","where"])[1][1],AnalyseCommand(command, ["select", "from","where"])[1][2],AnalyseCommand(command, ["select", "from","where"])[1][3],"select")
        if tmp[0]:
            db.select(tmp[2],tmp[1],tmp[3])
    elif AnalyseCommand(command, ["update", "set"])[0]:
        tmp = updete(AnalyseCommand(command, ["update", "set"])[1][2],AnalyseCommand(command, ["update", "set"])[1][1])
        if tmp[0]:
            db.update(tmp[3],tmp[2],tmp[1],"")
    elif AnalyseCommand(command, ["update", "set", "where"])[0]:
        tmp = updete(AnalyseCommand(command, ["update", "set","where"])[1][2],AnalyseCommand(command, ["update", "set","where"])[1][1])
        if tmp[0]:
            db.update(tmp[3],tmp[2],tmp[1],AnalyseCommand(command, ["update", "set", "where"])[1][3])
    elif AnalyseCommand(command, ["insert", "into", "values"])[0]:
        tmp=select_from(AnalyseCommand(command, ["insert", "into", "values"])[1][3],AnalyseCommand(command, ["insert", "into", "values"])[1][2])
        print(tmp)
        if tmp[0]:
            db.insert(tmp[2],tmp[1])
    elif AnalyseCommand(command, ["delete", "from", "where"])[0]:
        db.delete(AnalyseCommand(command, ["delete", "from", "where"])[1][2],AnalyseCommand(command, ["delete", "from", "where"])[1][3])
    elif AnalyseCommand(command, ["delete", "from"])[0]:
        db.delete(AnalyseCommand(command, ["delete", "from"])[1][2],"")
    elif AnalyseCommand(command,["create", "table"])[0]:
        tmp = create_table(AnalyseCommand(command,["create", "table"])[1][2])
        if tmp[0]:
            try:
                db.create_table(tmp[1],tmp[2],tmp[3],tmp[4])
            except:
                db.create_table(tmp[1],tmp[2],tmp[3])
    elif AnalyseCommand(command, ["drop", "table"])[0]:
        db.drop_table(AnalyseCommand(command, ["drop","table"])[1][2])
    elif AnalyseCommand("create database database1", ["create", "database"])[0]:
        current_database=DB.Database(AnalyseCommand("create database database1", ["create", "database"])[1][2])
    elif AnalyseCommand(command, ["select", "from", "inner", "join", "on"])[0]:
        tmp=AnalyseCommand(command, ["select", "from", "inner", "join", "on"])[1]
        db.inner_join(tmp[2],tmp[4],tmp[5])
    elif AnalyseCommand(command, ["create", "index", "on"])[0]:
        tmp=AnalyseCommand(command, ["create", "index", "on"])[1]
        print(tmp)
        db.create_index(tmp[2],tmp[3])


current_database=DB.Database("default_db")

filename="sql commands.txt"
f = open(filename,"r")
commands = f.read().replace("\n","").split(";")
print(commands)

for command in commands:
    try:
        execure_command(command,current_database)
    except:
       print("run time error")


