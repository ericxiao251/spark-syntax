
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
    .appName("Section 2.7 - Equality Statements in Spark and Comparison with Nulls")
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
  </tbody>
</table>
</div>



### Filtering Data

When you want ot filter data with more than just one expression, there are a couple of gotchas that you will need to be careful of.

### Case 1: Multiple Conditions


```python
(
    pets
    .where(
        (F.col('breed_id') == 1) &
        (F.col('color') == 'brown') &
        F.col('color').isin('brown')
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
  </tbody>
</table>
</div>



**What Happened?**

When there exists more than 1 condition you will to wrap each condition in `()` brackets and as well provide [bitwise operations](https://www.tutorialspoint.com/python/bitwise_operators_example.htm) instead of [logical operations](https://www.tutorialspoint.com/python/logical_operators_example.htm) in Python.

**Why?**

This is because in the spark internals they had to overwrite the `logical operations` and was only left with the `bitwise operations`. This is to my best knowledge, I could be wrong.

### Case 2: Nested Conditions


```python
(
    pets
    .where(
        (
            F.col('breed_id').isin([1, 2]) &
            F.col('breed_id').isNotNull()
        ) |
        (F.col('color') == 'white')
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
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

Similar to before, nested conditions will need to be wrapped with `()` as well.

### Case 3: Equality Statements with `Null` Values, (use `isNotNull()` and `isNull()`)


```python
(
    pets
    .withColumn('result', F.col('color') != 'white')
    .withColumn(
        'result_2', 
        (F.col('color') != 'white') &
        (F.col('color').isNotNull())
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
      <th>result</th>
      <th>result_2</th>
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
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
      <td>False</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

If you do not come from a `sql` background any comparison with `Null` will also be `Null`, unless you specifically use the `Null` comparisons.

The 2 `Null` comparisons are `isNotNull()` and `isNull()`.

### Summary

* In spark when using a more involved conditional expression, you will need to wrap each condition with `()` brackets and use the **bitwise operations** in Python.

* Be explicit with you're performing conditional transformations on columns that can be `Null`.
