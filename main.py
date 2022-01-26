import sys
import re
import csv
import numpy

def print_error(string):
    print(string)
    sys.exit()
def creating_dataset(filename):
    lines=open(filename,'r').readlines()
    dataset_to_table={}
    table_to_dataset={}
    lower_to_exact_column={}
    list_of_dataset=[]
    lower_to_exact_table={}
    dataset_name=""
    dataset_name_exact=""
    i=0
    while(i<len(lines)):
        row_without_split=lines[i].split('\n')[0]
        row=lines[i].split('\n')[0].lower()
        if(row=="<end_table>"):
            dataset_to_table[dataset_name]=list_of_dataset
        elif(row=="<begin_table>"):
            list_of_dataset=[]
            dataset_name_exact=lines[i+1].split('\n')[0]
            dataset_name=lines[i+1].split('\n')[0].lower()
            lower_to_exact_table[dataset_name]=dataset_name_exact
            i=i+1
        else:
            list_of_dataset.append(row)
            table_to_dataset[row]=(dataset_name,len(list_of_dataset))
            lower_to_exact_column[row]=row_without_split
        i=i+1
    return dataset_to_table,table_to_dataset,lower_to_exact_column,lower_to_exact_table
def error_fun(query):
    if(query[-1]!=';'):
        print_error("Expecting a ';' at the end")
    query=query[:-1].strip()
    if(re.match('select.*from*.',query)==None):
        print_error("select and from are not present or order between select and from is not followed")
    words_list=query.split()
    if(words_list[0]=='select'):
        words_list=words_list[1:]
    else:
        print_error("First Word is not select")
    return query,words_list
def list_tables_finding(words_list,given_list):
    columns_list=[]
    for i in range(0,len(words_list)):
        if(words_list[i] in given_list):
            words_list=words_list[i:]
            return columns_list,words_list
        list_here=words_list[i].split(',')
        for col in list_here:
            if col:
                columns_list.append(col)
        
    return columns_list,[]
#global data
def list_columns_finding(str_query):
    pos=str_query.find('from')
    column_str=str_query[6:pos].strip()
    pos=column_str.find('distinct')
    distinct_flag=0
    if(pos!=-1):
        column_str=column_str[pos+len('distinct'):]
        distinct_flag=1
    columns_list=column_str.strip().split(',')
    for i in range(0,len(columns_list)):
        data=columns_list[i].strip()
        data2=data.replace(" ","")
        if(len(data)!=len(data2)):
            data3=data.split()
            if(len(data3)==2):
                data=data3[1]
            else:
                print_error(", is delimtor in Columns but not spaces")
        columns_list[i]=data
    return columns_list,distinct_flag
def reading_table_from_file(table_name):
    dataset=[]
    if(table_name not in dataset_to_table):
        print_error(table_name+' not found in meta data')
    table_name_exact=lower_to_exact_table[table_name]
    red = open(table_name_exact+'.csv', 'r')
    csv_data=csv.reader(red,delimiter=',')
    csv_list=list(csv_data)
    for i in range(0,len(csv_list)):
        csv_list[i] = list(map(int, csv_list[i]))
    return csv_list,dataset_to_table[table_name]
def create_table_with_cols(our_dataset,columns_list,total_rows):
    csv_list_t_numpy=numpy.transpose(our_dataset)
    required_rows=[]
    for i in columns_list:
        if i in total_rows:
            pos=total_rows.index(i)
            if(len(csv_list_t_numpy)>pos):
                required_rows.append(csv_list_t_numpy[pos])
        else:
            print_error('`'+i+'`'+' not there in tables')
    csv_list=numpy.transpose(required_rows)
    return csv_list.tolist()

def print_me(data_set,columns_list):
    for i in range(len(columns_list)):
        if(columns_list[i] in table_to_dataset):
            columns_list[i]=lower_to_exact_table[table_to_dataset[columns_list[i]][0]]+'.'+lower_to_exact_column[columns_list[i]]
    print(','.join(columns_list))
    for i in data_set:
        str_list = [str(ii) for ii in i]
        print(','.join(str_list))
    if(len(data_set)==0):
        print("empty table")   
def join_two_tables(table1,table2):
    new_table=[]
    for i in table1:
        for j in table2:
            new_table.append(i+j)
    return new_table
def split_using_agg(columns_list):
    new_list=[]
    agg_list=[]
    agg_check_list=[]
    f=0
    for i in range(0,len(columns_list)):
        if('(' in columns_list[i]):
            index=columns_list[i].index('(')
            index2=columns_list[i].index(')')
            new_list.append(columns_list[i][index+1:index2])
            agg_list.append(columns_list[i][0:index])
            agg_check_list.append(columns_list[i][0:index])
        else:
            new_list.append(columns_list[i])
            agg_check_list.append('')
    return new_list,agg_check_list,agg_list
def creating_whole_data(tables_list):
    table_data,rows=reading_table_from_file(tables_list[0])
    for i in range(1,len(tables_list)):
        table_2data,rows2=reading_table_from_file(tables_list[i])
        table_data=join_two_tables(table_data,table_2data)
        rows=rows+rows2
    return table_data,rows
def get_int(string ,row_data,required_rows):
    try:
        return int(string)
    except:
        if string in required_rows:
            pos=required_rows.index(string)
            return row_data[pos]
        else:
            print_error("Given Column name is not found "+string)
def compartor_data(str1,str2,operation,row_data,required_rows):
    data1=get_int(str1,row_data,required_rows)
    data2=get_int(str2,row_data,required_rows)
    if(operation=='<' and data1 < data2):
        return True
    if(operation=='>' and data1 >  data2):
        return True
    if(operation=='<=' and data1 <=  data2):
        return True
    if(operation=='>=' and data1 >= data2):
        return True
    if(operation=='=' and data1 ==  data2):
        return True
    return False
def compartor(string,row_data,total_rows):
    checking_list=['<=','>=','>','<','=']
    for check in checking_list:
        pos=string.find(check)
        if (pos != -1):
            return compartor_data(string[0:pos],string[pos+len(check):],check,row_data,total_rows)
    print_error('Invalid operator')
def where_code(str1,str2,check,our_dataset,total_rows):
        new_data=[]
        for i in our_dataset:
            k1=compartor(str1,i,total_rows)
            k2=compartor(str2,i,total_rows)
            if(check=='or' and ( k1==True or k2 ==True)):
                new_data.append(i)
            if(check=='and' and (k1==True and k2==True)):
                new_data.append(i)
        return new_data
def where_function(words_list,our_dataset,total_rows):
    checker=['and','or']
    for check in checker:
        if check in words_list:
            pos1=words_list.index(check)
            str1=''.join(words_list[0:pos1])
            str2=''.join(words_list[pos1+1:])
            return where_code(str1,str2,check,our_dataset,total_rows)
    str1=''.join(words_list)
    return where_code(str1,str1,'or',our_dataset,total_rows)
def aggregates(operation,our_dataset,total_rows,column):
    if(column=='*'):
        if(operation=='count'):
            return len(our_dataset[0])
        else:
            print_error('Invalid Operation')
    if(column in total_rows):
        pos=total_rows.index(column)
        list_data=our_dataset[pos].tolist()
    else:
        print_error('Column is  not found '+column)
    if(operation=='max'):
        return max(list_data)
    if(operation=='min'):
        return min(list_data)
    if(operation=='sum'):
        return sum(list_data)
    if(operation=='count'):
        return len(list_data)
    if(operation=='avg'):
        if(len(list_data)):
            return sum(list_data)/len(list_data)
        else:
            print_error('Division by zero for avg')
    else:
        print_error('Invalid Operation')
#main code
dataset_to_table,table_to_dataset,lower_to_exact_column,lower_to_exact_table=creating_dataset('metadata.txt')
new_query=sys.argv[1].lower()
query=new_query.strip()
query,words_list=error_fun(query)
columns_list,distinct_flag=list_columns_finding(new_query)

#remove from
words_list=words_list[words_list.index('from')+1:]
tables_list,words_list=list_tables_finding(words_list,['where','group','order'])

if(len(tables_list)!=len(set(tables_list))):
    print_error("Tables are not unique")
our_dataset,total_rows=creating_whole_data(tables_list)
if '*' in columns_list:
    pos=columns_list.index('*')
    columns_list=columns_list[0:pos]+total_rows+columns_list[pos+1:]
columns_list,agg_check_list,agg_list=split_using_agg(columns_list)

flag_group=0
if('group' in words_list):
    flag_group=1
if(len(agg_list) and len(agg_check_list)!=len(agg_list) and 'group' not in words_list ):
        print_error('less number of aggregates')
#where code
if(len(words_list)and words_list[0]=='where'):
        words_list=words_list[1:]
        compare_list,words_list=list_tables_finding(words_list,['group','order'])
        our_dataset=where_function(compare_list,our_dataset,total_rows)
#order by code
column_to_group=''
if(len(words_list) and words_list[0]=='group'):
    words_list=words_list[1:]
    if(len(words_list) and  words_list[0]=='by'):
        words_list=words_list[1:] 
        column_to_group,words_list=list_tables_finding(words_list,['order'])
        if(len(column_to_group)==0):
            print_error('Column to group is not specified')
        if(len(column_to_group)>1):
            print_error('More columns are given  to group syntex error')
        column_to_group=column_to_group[0]
    else:
        print_error('Group is not followed by by')
    #only single column for
if(len(words_list) and words_list[0]=='order'):
    words_list=words_list[1:]
    if(len(words_list) and words_list[0]=='by'):
        words_list=words_list[1:]
        if(len(words_list)>2 or len(words_list)==0):
            print_error('SQL syntex error')
        else:
                order=1
                if(len(words_list)==2):
                    if( words_list[1]=='desc'):
                        order=-1
                    elif(words_list[1]=='asc'):
                        order=1
                    else:
                        print_error("Only ASC and DESC are allowed key words for Order by")
                if(words_list[0] in total_rows):
                    index=total_rows.index(words_list[0])
                    our_dataset=sorted(our_dataset,key=lambda x:order*x[index])
                else:
                    s=words_list[0].split(',')
                    if(len(s)>=2):
                       print_error('More columns are given a syntex error')
                    else:
                       print_error("Specified column is not there")
    else:
        print_error('Order is not followed by by')
if(len(agg_list) and flag_group==0):
    value_list=[]
    csv_list_t_numpy=numpy.transpose(our_dataset)
    new_list=[]
    #completely code here
    for i in range(0,len(columns_list)):
        value=aggregates(agg_list[i],csv_list_t_numpy,total_rows,columns_list[i])
        value_list.append(str(value))
        if(columns_list[i] in lower_to_exact_column):
            new_list.append(agg_list[i]+'('+ lower_to_exact_column[columns_list[i]]+')')
        else:
            new_list.append(agg_list[i]+'('+columns_list[i]+')')
    print(','.join(new_list))
    print(','.join(value_list))
    sys.exit()
if(flag_group==0):
    our_dataset=create_table_with_cols(our_dataset,columns_list,total_rows)
else:
    #create own dataset
    if(column_to_group in total_rows):
        pos=total_rows.index(column_to_group)
        dictionery={}
        list_t=numpy.transpose(our_dataset).tolist()
        for i in range(0,len(list_t[pos])):
            if list_t[pos][i] not in dictionery:
                dictionery[list_t[pos][i]]=[]
            dictionery[list_t[pos][i]].append(our_dataset[i])
        new_data=[]
        new_column_list=[]
        
        for i in dictionery.keys():
            our_new_dataset=dictionery[i]
            our_new_t=numpy.transpose(our_new_dataset)
            new_row=[]
            for i in range(0,len(columns_list)):
                if(agg_check_list[i]==''):
                    if(columns_list[i] in total_rows):
                        if(columns_list[i]==column_to_group):
                            position=total_rows.index(columns_list[i])
                            new_row.append(our_new_dataset[0][position])
                        else:
                            print_error("Cant group by as in valid column "+columns_list[i])
                    else:
                        print_error("Specified column is not there "+columns_list[i])
                else:
                    value=aggregates(agg_check_list[i],our_new_t,total_rows,columns_list[i])
                    new_row.append(value)
        
            new_data.append(new_row)
        for i in range(0,len(columns_list)):
            if(agg_check_list[i]==''):
                new_column_list.append(columns_list[i])
            else:
                if(columns_list[i] in lower_to_exact_column):
                    new_column_list.append(agg_check_list[i]+'('+lower_to_exact_column[columns_list[i]]+')')
                else:
                    new_column_list.append(agg_check_list[i]+'('+columns_list[i]+')')
        our_dataset=new_data
        columns_list=new_column_list
    else:
        print_error('Specified column to group is not present')
if(distinct_flag==1):
    total_datasets=[]
    dictionery={}
    for i in our_dataset:
        dictionery[tuple(i)]=1
    for i in dictionery.keys():
        total_datasets.append(list(i))
    our_dataset=total_datasets
print_me(our_dataset,columns_list)
