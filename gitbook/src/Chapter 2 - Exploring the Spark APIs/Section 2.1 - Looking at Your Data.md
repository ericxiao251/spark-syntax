
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
    .appName("Section 2.1 - Looking at Your Data")
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
  </tbody>
</table>
</div>



### Looking at Your Data

Spark is lazily evaluated. To look at your data you must perform a `take` operation to trigger your transformations to be evaluated. There are a couple of ways to perform a `take` operation that we'll go through here, and their performance.

For example, the `toPandas()` is a `take` operation which you've already seen in many places.

### Option 1 - `collect()`


```python
pets.collect()
```




    [Row(id=u'1', breed_id=u'1', nickname=u'King', birthday=u'2014-11-22 12:30:31', age=u'5', color=u'brown'),
     Row(id=u'2', breed_id=u'3', nickname=u'Argus', birthday=u'2016-11-22 10:05:10', age=u'10', color=None),
     Row(id=u'3', breed_id=u'1', nickname=u'Chewie', birthday=u'2016-11-22 10:05:10', age=u'15', color=None)]



**What Happened?**

When you call `collect` on a `dataframe`, it will trigger a `take` operation, bring all the data to the driver node and then return all rows as a lists of `Row` objects.

**Note**

This should not be advised unless you **have to** look at all the rows of your dataset, you should usually sample a subset of the data. This call will execution **all** of the transformations that you have specified on **all** the data.

### Option 2 - `head()/take()/first()`


```python
pets.head(n=1)
```




    [Row(id=u'1', breed_id=u'1', nickname=u'King', birthday=u'2014-11-22 12:30:31', age=u'5', color=u'brown')]



**What Happened?**

When you call `head(n)` on a `dataframe`, it will trigger a `take` operation and return the first `n` rows of the result dataset. The different operations will return different number of rows.

**Note**

* If the data is **unsorted**, spark will perform the **all** the transformations on a selected amount of partitions until the number of rows are satified. This is much optimal based on how much and large your dataset is.
* If the data is **sorted**, spark will perform the same as a `collect` and perform `all` of the `transformations` on `all` of the data.

By `sorted` we mean, if any sort of "sorting of the data" is done during the transformations, such as `sort()`, `orderBy()`, etc.

### Option 3 - `toPandas()`


```python
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
  </tbody>
</table>
</div>



**What Happened?**

When you call a `toPandas()` on a `dataframe`, it will trigger a `take` operation and return all of the rows.

This is as performant as the `collect()` function, but the most readible in my opinion.

### Option 4 - `show()`


```python
pets.show()
```

    +---+--------+--------+-------------------+---+-----+
    | id|breed_id|nickname|           birthday|age|color|
    +---+--------+--------+-------------------+---+-----+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|
    +---+--------+--------+-------------------+---+-----+
    


**What Happened?**

When you call a `show()` on a `dataframe`, it will trigger a `take` operation return up to 20 rows.

This is as performant as the `head()` function and more readible. (I still perfer `toPandas()` 😀).

### Summary

* We learnt about various functions that allow you to look at your data.
* Some functions are less performant than others based on if the resultant data is sorted or not.
* Try to refrain from looking at all the data, unless you are required to.
