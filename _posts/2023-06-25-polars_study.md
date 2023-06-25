---
layout: single
title:  "Python_package_Polars"
categories: python
tag: [python, data, polars]
toc: true
author_profile: false
---

<head>
  <style>
    table.dataframe {
      white-space: normal;
      width: 100%;
      height: 240px;
      display: block;
      overflow: auto;
      font-family: Arial, sans-serif;
      font-size: 0.9rem;
      line-height: 20px;
      text-align: center;
      border: 0px !important;
    }

    table.dataframe th {
      text-align: center;
      font-weight: bold;
      padding: 8px;
    }

    table.dataframe td {
      text-align: center;
      padding: 8px;
    }

    table.dataframe tr:hover {
      background: #b8d1f3; 
    }

    .output_prompt {
      overflow: auto;
      font-size: 0.9rem;
      line-height: 1.45;
      border-radius: 0.3rem;
      -webkit-overflow-scrolling: touch;
      padding: 0.8rem;
      margin-top: 0;
      margin-bottom: 15px;
      font: 1rem Consolas, "Liberation Mono", Menlo, Courier, monospace;
      color: $code-text-color;
      border: solid 1px $border-color;
      border-radius: 0.3rem;
      word-break: normal;
      white-space: pre;
    }

  .dataframe tbody tr th:only-of-type {
      vertical-align: middle;
  }

  .dataframe tbody tr th {
      vertical-align: top;
  }

  .dataframe thead th {
      text-align: center !important;
      padding: 8px;
  }

  .page__content p {
      margin: 0 0 0px !important;
  }

  .page__content p > strong {
    font-size: 0.8rem !important;
  }

  </style>
</head>


### Polars는 RUST 기반으로 개발된 데이터 분석 도구임. 



```python
# 오프라인 환경 패키지 설치. 
# pip install --no-index --find-links=\\apkrp-wfsr193\DA\GH\프로그램\polars polars
# !conda install --no-index --find-links=C:/Users/gukim00/Desktop/csv_file/polar connectorx
```


```python
import polars as pl # polars import 
import connectorx as cx # connectorx 는 polars 가 데이터 프레임에 접근하는 도구이며,
ip_address = '123.123.123.132'
port = '1000'
db = 'test_db' 
conn = f'mssql+pyodbc://{ip_address}:{port}/{db}?trusted_connection=true'  # 윈도우 인증 로그인의 경우 trusted_connection= true 를 준다.
```


```python
## pandas pd.read_sql 처럼 포트번호를 지정해주고 engine 만 connectorx 로 지정해주면 된다. 
query = "select * from ProductRecommender where yearmonth = '202306' AND STATUS = 'ACTIVE'"
df = pl.read_database(query,  connection_uri=conn, engine = 'connectorx')
file_path = '파일경로입력'
df.write_csv(file_path)
```

lazy 프레임에 넣는 방법.

- lazy 프레임이란. 데이터를 램에 넣기 전에 불필요한 항목들을 미리 제거하고 DataFrame 에 담는 방법.

- 기존 pandas의 경우 데이터 가공을 위해 전데이터를 램에 넣어야 했음. 

- https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html



```python

### sql query 에서 데이터를 불러오고 해당 데이터를 lazy_frame 에 담는다. 
df2 = pl.read_database('select * from db_table' , conn).lazy()

## 일단 sql을 통해 호출을하고 
```


```python
### lazy frame의 경우 head 대신에 fetch()를 활용한다. 
df2.fetch(4)
```

<pre>
1
</pre>

```python
## 필터로 10개만 보기. collect()실행을 한번했더니 램용량이 많이 올라감....
df2.filter((pl.col('customerInsuranceAge') > 70) & 
           (pl.col('productcode').str.contains('CAN'))
           ).fetch(2)
```

<pre>
1
</pre>

```python
## apply함수의 경우에는 일반 pandas 와는 조금 다른 방법인것 같다.
## pandas 의 경우 직접적으로 컬럼에 apply 를 하게되면 series 형태로 반환을 했다면 polars 의 경우 검색을 해봐도 with_columns과 같이 새 컬럼을 생성해야 적용이 된다.
df = pl.DataFrame({'A':[1,2,3,4,5],
                   'B':[5,6,7,8,9]
                   })

def max_of_column(column): 
  if column == 1 : 
    return df['B']

result = df.with_columns(pl.col('A').apply(max_of_column).alias('applied_max'))

print(result)
```

<pre>
shape: (5, 3)
┌─────┬─────┬─────────────┐
│ A   ┆ B   ┆ applied_max │
│ --- ┆ --- ┆ ---         │
│ i64 ┆ i64 ┆ list[i64]   │
╞═════╪═════╪═════════════╡
│ 1   ┆ 5   ┆ [5, 6, … 9] │
│ 2   ┆ 6   ┆ null        │
│ 3   ┆ 7   ┆ null        │
│ 4   ┆ 8   ┆ null        │
│ 5   ┆ 9   ┆ null        │
└─────┴─────┴─────────────┘
</pre>

```python
processed_segment = df.with_columns(pl.when(pl.col('A')> 2).then(pl.col('B')).otherwise(0).alias('131'))
```


```python
processed_segment
```

<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
}
</style>
<small>shape: (5, 3)</small><table border="1" class="dataframe"><thead><tr><th>A</th><th>B</th><th>131</th></tr><tr><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>1</td><td>5</td><td>0</td></tr><tr><td>2</td><td>6</td><td>0</td></tr><tr><td>3</td><td>7</td><td>7</td></tr><tr><td>4</td><td>8</td><td>8</td></tr><tr><td>5</td><td>9</td><td>9</td></tr></tbody></table></div>


아래는 sqlalchemy를 이용한 쿼리 방법



```python
import pymssql
import numpy as np
import pandas as pd
import datetime 
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import sys, os 
import sqlalchemy as db
from sqlalchemy import select ,Table, func, text
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker


db_url = URL.create(
        # drivername="mssql+pymssql",
        drivername="mssql+pyodbc",
        host = "ip_address_here",
        port  = 'port_here',
        database = "db_here",
        query = {
                "driver" : "ODBC Driver 13 for SQL Server",
                "TrustServerCertificate" : "yes",
                "authentication" : "ActiveDirectoryIntegrated",
                "isolation_level" : "AUTOCOMMIT" ## autocommit 설정 (아래 엔진에서 설정해도 무관.)
        }
)

db_engine = create_engine(db_url,encoding = 'utf-8' ,isolation_level = 'AUTOCOMMIT')


metadata =  db.MetaData()


connection = db_engine.raw_connection()
cursor = connection.cursor()

```


```python
#방법 1 단순 READ 
sql = "select top 1 * from db_table where yearmonth = '202306' AND STATUS = 'ACTIVE'"
reco_df = pd.read_sql(sql, connection)


# 방법 2 임시테이블 생성등등에 활용 EX(INSERT INTO)
cursor.execute(sql)
rows = cursor.fetchall() #fetchall 의경우 리스트의 형식으로 데이터를 담기때문에 컬럼 설정이 되지 않는다. 컬럼은 아래와 같이 일괄 가져올 수 있음.
columns = [column[0] for column in cursor.description]

df = pd.DataFrame.from_records(rows,columns=columns)
```

<pre>
c:\Users\gukim00\Anaconda3\envs\py39_clone\lib\site-packages\pandas\io\sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
  warnings.warn(
</pre>

```python
df = pd.DataFrame.from_records(rows.fetchall(),columns=columns)
```


```python
# 이유는 모르겠는데 polars CONNECTORX 실행후 SQLALCHEMY 실행시 호스트 연결이 끊긴다는 에러가 발생함. 
```


```python
import pyodbc 
print(pyodbc.drivers())
```

<pre>
['SQL Server', 'Microsoft Access Driver (*.mdb, *.accdb)', 'Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)', 'Microsoft Access Text Driver (*.txt, *.csv)', 'SQL Server Native Client 11.0', 'ODBC Driver 13 for SQL Server', 'Microsoft Access dBASE Driver (*.dbf, *.ndx, *.mdx)']
</pre>
