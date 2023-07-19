# Cell on Cluster Busy Hour

## Import Required Libraries


```python
import os
import zipfile
import pandas as pd
from glob import glob
import dask.dataframe as dd
from datetime import datetime
```

## Check Start Time of the Script


```python
from datetime import datetime
start_time = datetime.now()
print(start_time)
```

    2023-07-18 15:55:54.753432
    

## Import CDF File


```python
working_directory = 'D:/Advance_Data_Sets/KPIs_Analysis/GSM_Cell_on_Cluster_BH/CDF'
os.chdir(working_directory)
df= pd.read_excel('cdf.xlsx', dtype={'Cell CI': str})
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>GBSC</th>
      <th>Cell CI</th>
      <th>Cluster</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>HBWPBSC02</td>
      <td>14156</td>
      <td>AHMEDPUREAST_CLUSTER_01_Rural</td>
    </tr>
    <tr>
      <th>1</th>
      <td>HBWPBSC02</td>
      <td>24156</td>
      <td>AHMEDPUREAST_CLUSTER_01_Rural</td>
    </tr>
    <tr>
      <th>2</th>
      <td>HBWPBSC02</td>
      <td>34156</td>
      <td>AHMEDPUREAST_CLUSTER_01_Rural</td>
    </tr>
    <tr>
      <th>3</th>
      <td>HBWPBSC02</td>
      <td>24155</td>
      <td>AHMEDPUREAST_CLUSTER_01_Rural</td>
    </tr>
    <tr>
      <th>4</th>
      <td>HBWPBSC02</td>
      <td>34616</td>
      <td>AHMEDPUREAST_CLUSTER_01_Rural</td>
    </tr>
  </tbody>
</table>
</div>



## Import Cluster Busy Hour File


```python
working_directory = 'D:/Advance_Data_Sets/KPIs_Analysis/GSM_Cell_on_Cluster_BH/ClusterBH'
os.chdir(working_directory)
# import cluster BH File
df1= pd.read_excel('clusterbh_c.xlsx',parse_dates=['Date']) 
df1['Time'] = df1['Time'].apply(lambda t: t.strftime('%H:%M'))
df1.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>Time</th>
      <th>Cluster</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-04-01</td>
      <td>11:00</td>
      <td>DG_KHAN_CLUSTER_02_Urban</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-04-01</td>
      <td>11:00</td>
      <td>GUJRANWALA_CLUSTER_01_Urban</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-04-01</td>
      <td>11:00</td>
      <td>JAMPUR_CLUSTER_01_Urban</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-04-01</td>
      <td>11:00</td>
      <td>JHUNG_CLUSTER_05_Urban</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-04-01</td>
      <td>11:00</td>
      <td>KHANPUR_CLUSTER_01_Urban</td>
    </tr>
  </tbody>
</table>
</div>



## Cell Hourly Data Set


```python
# Set File Path
working_directory = 'D:/Advance_Data_Sets/KPIs_Analysis/GSM_Cell_on_Cluster_BH/CellHourly'
os.chdir(working_directory)

# # Unzip Files
# for file in os.listdir(working_directory):   # get the list of files
#     if zipfile.is_zipfile(file): # if it is a zipfile, extract it
#         with zipfile.ZipFile(file) as item: # treat the file as a zip
#            item.extractall()  # extract it in the working directory
```

### a) Import csv Files in Dask DataFrame


```python
df2 = dd.read_csv('*.csv',\
                   skiprows=range(6),
                   skipfooter=1,
                    engine='python',\
                    na_values=['NIL','/0'],
                      usecols=['Date','Time',
                            'GBSC','Cell CI',
                            'CSSR_Non Blocking_1_N','CSSR_Non Blocking_2_N',
                            'CSSR_Non Blocking_1_D','CSSR_Non Blocking_2_D',
                            '_DCR_N','_DCR_D',
                            '_HSR%_N','_HSR%_D',
                            'N_OutHSR_D','N_OutHSR_N',
                            '_RxQual Index DL_1','_RxQual Index DL_2',
                            '_RxQual Index UL_1','_RxQual Index UL_2',
                            '_CallSetup TCH GOS(%)_N','_CallSetup TCH GOS(%)_D',
                            '_Mobility TCH GOS(%)_N','_Mobility TCH GOS(%)_D',
                            '_GOS-SDCCH(%)_N','_GOS-SDCCH(%)_D'],
                              dtype={
                            'GBSC': 'object',
                            'Cell CI': 'object',
                            'CSSR_Non Blocking_1_N': 'object',
                            'CSSR_Non Blocking_2_N': 'object',
                            'CSSR_Non Blocking_1_D': 'object',
                            'CSSR_Non Blocking_2_D': 'object',
                            '_DCR_N': 'object',
                            '_DCR_D': 'object',
                            '_HSR%_N': 'object',
                            '_HSR%_D': 'object',
                            'N_OutHSR_D': 'object',
                            'N_OutHSR_N': 'object',
                            '_RxQual Index DL_1': 'object',
                            '_RxQual Index DL_2': 'object',
                            '_RxQual Index UL_1': 'object',
                            '_RxQual Index UL_2': 'object',
                            '_CallSetup TCH GOS(%)_N': 'object',
                            '_CallSetup TCH GOS(%)_D': 'object',
                            '_Mobility TCH GOS(%)_N': 'object',
                            '_Mobility TCH GOS(%)_D': 'object',
                            '_GOS-SDCCH(%)_N': 'object',
                            '_GOS-SDCCH(%)_D': 'object'})
```


```python
# Convert 'Date' and 'Time' columns to datetime
df2['Date'] = dd.to_datetime(df2['Date'])
```

### c) Merge Cell Hourly Data with CDF


```python
# get the cluster name from cdf
df3 = pd.merge(df2.compute(), df[['GBSC','Cell CI','Cluster']], on=['GBSC','Cell CI'], how='right')
# convert to dask
df4 = dd.from_pandas(df3, npartitions=df2.npartitions)
```

### d) Filter Cell on Cluster BH KPIs


```python
df5= dd.merge(df4,df1,on=['Cluster','Date','Time'])
```

## Compute Date Set


```python
df5 = df5.compute()
```

## Remove Duplicates


```python
df5 = df5.drop_duplicates()
```

## Output Data Set


```python
working_directory = 'D:/Advance_Data_Sets/KPIs_Analysis/GSM_Cell_on_Cluster_BH/Output'
os.chdir(working_directory)

for f in os.listdir(working_directory):
    os.remove(os.path.join(working_directory, f))

for i, g in df5.groupby('Cluster'):
    g.to_csv('Cluster_{}.csv'.format(i), header=True,index=False)
```

## Delete Unzip Files


```python
path_cell_hr = 'D:/Advance_Data_Sets/KPIs_Analysis/GSM_Cell_on_Cluster_BH/CellHourly'
os.chdir(path_cell_hr)

for filename in os.listdir(path_cell_hr):
    if filename.endswith('.csv'):
        os.unlink(os.path.join(path_cell_hr, filename))  
```

## Check End Time of the Script


```python
end_time = datetime.now()
print(end_time)
```

    2023-07-18 16:08:50.389785
    


```python
tt =end_time-start_time
```


```python
tt
```




    datetime.timedelta(seconds=775, microseconds=636353)




```python

```
