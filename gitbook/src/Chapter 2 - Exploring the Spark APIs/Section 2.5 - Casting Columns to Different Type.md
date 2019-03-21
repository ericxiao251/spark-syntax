
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
    .appName("Section 2.5 - Casting Columns to Different Type")
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



### Casting Columns in Different Types

Sometimes your data can be read in as all `unicode`/`string` in which you will need to cast them to the correct type. Or Simply you want to change the type of a column as a part of your transformation.

### Option 1 - `cast()`


```python
(
    pets
    .select('birthday')
    .withColumn('birthday_date', F.col('birthday').cast('date'))
    .withColumn('birthday_date_2', F.col('birthday').cast(T.DateType()))
    .toPandas()
)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>birthday</th>
      <th>birthday_date</th>
      <th>birthday_date_2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2014-11-22 12:30:31</td>
      <td>2014-11-22</td>
      <td>2014-11-22</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2016-11-22 10:05:10</td>
      <td>2016-11-22</td>
      <td>2016-11-22</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2016-11-22 10:05:10</td>
      <td>2016-11-22</td>
      <td>2016-11-22</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

There are 2 ways that you can `cast` a column.
1. Use a string (`cast('date')`).
2. Use the spark types (`cast(T.DateType())`).

I tend to use a string as it's shorter, one less import and in more editors there will be syntax highlighting for the string. 

### Summary

* We learnt about two ways of casting a column.
* The first way is a bit more cleaner IMO.
