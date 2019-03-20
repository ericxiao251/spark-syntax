
When dealing with big data, some datasets will have a much higher frequent of "events" than others.

An example table could be a table that tracks each pageview, it's not uncommon for someone to visit a site at the same time as someone else, espically a very popular site such as google.

I will illustrate how you can deal with these types of events, when you need to order by time.

### Library Imports


```python
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Exploring Joins")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext
```

### Option 1: Only ordering by date column


```python
window = (
    Window
    .partitionBy('breed_id')
    .orderBy('birthday')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
```


```python
pets = spark.createDataFrame(
    [
        (1, 1, datetime(2018, 1, 1, 1 ,1, 1), 45),
        (2, 1, datetime(2018, 1, 1, 1 ,1, 1), 20),
    ], ['id', 'breed_id', 'birthday', 'age']
)

pets.withColumn('first_pet_of_breed', F.first('id').over(window)).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>birthday</th>
      <th>age</th>
      <th>first_pet_of_breed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>45</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>20</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
pets = spark.createDataFrame(
    [
        (2, 1, datetime(2018, 1, 1, 1 ,1, 1), 20),
        (1, 1, datetime(2018, 1, 1, 1 ,1, 1), 45),
    ], ['id', 'breed_id', 'birthday', 'age']
)

pets.withColumn('first_pet_of_breed', F.first('id').over(window)).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>birthday</th>
      <th>age</th>
      <th>first_pet_of_breed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>20</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>45</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened:**
* By changing the order of rows (this would happen with larger amounts of data stored on different partitions), we got a different value for "first" value.
* `datetime`s can only be accurate to the second and if data is coming in faster than that, it is ambiguous to order by the date column.

### Option 2: Order by `date` and `id` Column

### Window Object


```python
window_2 = (
    Window
    .partitionBy('breed_id')
    .orderBy('birthday', 'id')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
```


```python
pets = spark.createDataFrame(
    [
        (1, 1, datetime(2018, 1, 1, 1 ,1, 1), 45),
        (2, 1, datetime(2018, 1, 1, 1 ,1, 1), 20),
    ], ['id', 'breed_id', 'birthday', 'age']
)

pets.withColumn('first_pet_of_breed', F.first('id').over(window_2)).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>birthday</th>
      <th>age</th>
      <th>first_pet_of_breed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>45</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>20</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
pets = spark.createDataFrame(
    [
        (2, 1, datetime(2018, 1, 1, 1 ,1, 1), 20),
        (1, 1, datetime(2018, 1, 1, 1 ,1, 1), 45),
    ], ['id', 'breed_id', 'birthday', 'age']
)

pets.withColumn('first_pet_of_breed', F.first('id').over(window_2)).toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>birthday</th>
      <th>age</th>
      <th>first_pet_of_breed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>45</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>20</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened**:
* We get the same "first" value in both incidents, which is what we expect.

# TL;DR

In databases, the `id` (primary key) column of a table is usually monotonically increasing. Therefore if we are dealing with frequently arriving data we can additionally sort by `id` along the `date` column.
