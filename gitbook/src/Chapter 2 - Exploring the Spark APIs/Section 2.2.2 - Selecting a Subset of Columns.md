
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



### Selecting a Subset of Columns

When you're working with raw data, you are usually only interested in a subset of columns. This means you should get into the habit of only selecting the columns you need before any spark transformations.

**Why?**

If you do not, and you are working with a wide dataset this will cause your spark application to do more work than it should do. This is because all the extra columns will  be `shuffled` between the worker during the execution of the transformations. 

**This will really kill you, if you have string columns that have really large amounts of text within them.**

**Note**

Spark is sometimes smart enough to know which columns aren't being used and perform a `Project Pushdown` to drop the unneeded columns. But it's better practice to do the selection first. 

### Option 1 - `select()`


```python
(
    pets
    .select("id", "nickname", "color")
    .toPandas()
)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>nickname</th>
      <th>color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>King</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Argus</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Chewie</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

Similar to a `sql select` statement, it will only keep the columns specified in the arguments in the resulting `df`. a `list` object can be passed as the argument as well.

If you have a wide dataset and only want to work with a small number of columns, a `select` would be less lines of code.

**Note**

If the argument `*` is provided, all the columns will be selected.

### Option 2 - `drop()`


```python
(
    pets
    .drop("breed_id", "birthday", "age")
    .toPandas()
)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>nickname</th>
      <th>color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>King</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Argus</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Chewie</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

This is the opposite of a `select` statement it will drop an of the columns specified.

If you have a wide dataset and will need a majority of the columns, a `drop` would be less lines of code.

### Summary

* Work with only the subset of columns required for your spark application, there is no need do extra work.
* Depending on the number of columns you are going to work with, a `select` over a `drop` would be better and vice-versa.
