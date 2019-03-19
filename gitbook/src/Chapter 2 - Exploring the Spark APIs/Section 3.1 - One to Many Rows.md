
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from pyspark.sql import functions as F

from datetime import datetime
from decimal import Decimal
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Section 3.1 - One to Many Rows")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext

import os

data_path = "/data/pets.csv"
base_path = os.path.dirname(os.getcwd())
path = base_path + data_path
```


```python
pets = spark.read.csv(path, header=True)
pets.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>2</td>
      <td>None</td>
      <td>2019-01-01 10:05:10</td>
      <td>13</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



### One to Many Rows

Very commonly you might have a column where it is an `array` type and you want to flatten that array out into multiple rows? Well let's look at how we can do that and some practical applications of it.

## Multiple People Interested in Babysitting the Same Pet

### Case 1: Get a Table with just the People Interested

Question to answer:

We have a couple of people interested in our little friend named "King", we want to create a new dataset containing a single row for each people interested in King. How can I do this?


```python
pets_2 = (
    pets
    .where(F.col('id') == 1)
    .withColumn(
        'people_interested',
        F.array([
            F.lit('John'),
            F.lit('Doe'),
            F.lit('Bob'),
            F.lit('Billy')
        ])
    )
)

pets_2.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
      <th>people_interested</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>[John, Doe, Bob, Billy]</td>
    </tr>
  </tbody>
</table>
</div>




```python
(
    pets_2
    .withColumn('people_interested', F.explode(F.col('people_interested')))
    .toPandas()
)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
      <th>people_interested</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>John</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>Doe</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>Bob</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>Billy</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

1. So we first created a column that contained a list of names of the people interested.
2. Next we split the list so that there is a row per person interested.

### Case 2: Get a Table with just the People Interested and the Number of Days

Question to answer:

We have a couple of people interested in our little friend named "King" and the number of days they would like to babysit for, we want to create a new dataset containing a single row for each people interested in King. How can I do this?


```python
pets_2 = (
    pets
    .where(F.col('id') == 1)
    .withColumn(
        'people_interested',
        F.array([
            F.create_map([F.lit('John'), F.lit(5)]),
            F.create_map([F.lit('Doe'), F.lit(3)]),
            F.create_map([F.lit('Bob'), F.lit(7)]),
            F.create_map([F.lit('Billy'), F.lit(9)])
        ])
    )
)

pets_2.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
      <th>people_interested</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>[{u'John': 5}, {u'Doe': 3}, {u'Bob': 7}, {u'Bi...</td>
    </tr>
  </tbody>
</table>
</div>




```python
(
    pets_2
    .withColumn('people_interested', F.explode(F.col('people_interested')))
    .select(
        "*", 
        F.explode('people_interested').alias('person', 'days')
    )
    .toPandas()
)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
      <th>people_interested</th>
      <th>person</th>
      <th>days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>{u'John': 5}</td>
      <td>John</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>{u'Doe': 3}</td>
      <td>Doe</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>{u'Bob': 7}</td>
      <td>Bob</td>
      <td>7</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>{u'Billy': 9}</td>
      <td>Billy</td>
      <td>9</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

1. So we first created a column that contained a list of dictionary mapping the name of the person interested with the number of days they were interested.
2. Here we had to explode twice, first to get a row per person interested (table grew longer), then to split each field of the dictioanry to get the name of the person and the number of days (table grew wider).

### Summary

* We looked at some pretty complex transformations that involved using the `explode` function which decomposed an `array`/`map` object into multiple rows/columns.
