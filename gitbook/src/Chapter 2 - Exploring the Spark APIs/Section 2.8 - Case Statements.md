
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
    .appName("Section 2.8 - Case Statements")
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



### Case Statements

Case statements are usually used for performing stateful calculations. 

ie. 
- if `x` then `a`
- if `y` then `b`
- everything else `c`

### Using Switch/Case Statements  in `Spark`


```python
(
    pets
    .withColumn(
        'oldness_value',
        F.when(F.col('age') <= 5, 'young')
         .when((F.col('age') > 5) & (F.col('age') <= 10), 'middle age')
         .otherwise('old')
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
      <th>oldness_value</th>
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
      <td>young</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
      <td>middle age</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>old</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
      <td>old</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>2</td>
      <td>None</td>
      <td>2019-01-01 10:05:10</td>
      <td>13</td>
      <td>None</td>
      <td>old</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

Based on the age of the pet, we classified if they are either `young`, `middle age` or `old`. **Please don't take offense, this is merely an example. **

We mapped the logic of:
- If their age is younger than or equal to 5, then they are considered `young`.
- If their age is greater than 5 but younger than or equal to 10 , then they are considered `middle age`.
- Anyone older is considered `old`.

### Summary

* We learned how to map values based on case statements and a deafult value if all conditions are not satified.
