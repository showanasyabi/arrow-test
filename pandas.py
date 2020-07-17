import pandas as pd
import datetime

df=pd.read_csv('panda_13.csv')
t1 = datetime.datetime.now()
var =df.groupby(['sex'])['age'].sum()
t2= datetime.datetime.now()
print(var)
exe_time= t2-t1
print(exe_time.microseconds)
