
### Introduction

There are use cases where we would like to get the `first` or `last` of something within a `group` or particular `grain`.

It is natural to do something in SQL like:

```sql
select 
    col_1,
    first(col_2) as first_something,
    last(col_2) as first_something
from table
group by 1
order by 1
```

Which leads us to writing spark code like this `df.orderBy().groupBy().agg()`. This has unexpected behaviours in spark and can be different each run.

### Library Imports


```python
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
```

Create a `SparkSession`. No need to create `SparkContext` as you automatically get it as part of the `SparkSession`.


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

### Initial Datasets


```python
pets = spark.createDataFrame(
    [
        (1, 1, datetime(2018, 1, 1, 1 ,1, 1), 'Bear', 5),
        (2, 1, datetime(2010, 1, 1, 1 ,1, 1), 'Chewie', 15),
        (3, 1, datetime(2015, 1, 1, 1 ,1, 1), 'Roger', 10),
    ], ['id', 'breed_id', 'birthday', 'nickname', 'age']
)

pets.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>birthday</th>
      <th>nickname</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>Bear</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2010-01-01 01:01:01</td>
      <td>Chewie</td>
      <td>15</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>2015-01-01 01:01:01</td>
      <td>Roger</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>



### Option 1: Wrong Way

#### Result 1


```python
df_1 = (
    pets
    .orderBy('birthday')
    .groupBy('breed_id')
    .agg(F.first('nickname').alias('first_breed'))
)

df_1.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>first_breed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Chewie</td>
    </tr>
  </tbody>
</table>
</div>



#### Result 2


```python
df_2 = (
    pets
    .orderBy('birthday')
    .groupBy('breed_id')
    .agg(F.first('nickname').alias('first_breed'))
)

df_2.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>first_breed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Chewie</td>
    </tr>
  </tbody>
</table>
</div>



### Option 2: Window Object, Right Way


```python
window = Window.partitionBy('breed_id').orderBy('birthday')

df_3 = (
    pets
    .withColumn('first_breed', F.first('nickname').over(window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    .withColumn('rn', F.row_number().over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
)

df_3.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>birthday</th>
      <th>nickname</th>
      <th>age</th>
      <th>first_breed</th>
      <th>rn</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>1</td>
      <td>2010-01-01 01:01:01</td>
      <td>Chewie</td>
      <td>15</td>
      <td>Chewie</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>1</td>
      <td>2015-01-01 01:01:01</td>
      <td>Roger</td>
      <td>10</td>
      <td>Chewie</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>2018-01-01 01:01:01</td>
      <td>Bear</td>
      <td>5</td>
      <td>Chewie</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



### Summary

Ok so my example didn't work locally lol, but trust me it that `orderBy()` in a statement like this: `orderBy().groupBy()` doesn't maintain it's order!

reference: https://stackoverflow.com/a/50012355

For anything aggregation that needs an ordering performed (ie. `first`, `last`, etc.), we should avoid using `groupby()`s and instead we should use a `window` object.
