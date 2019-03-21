
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
    .appName("Section 2.9 - Filling in Null Values")
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



### Filling in Null Values

Working with real world data, some information can be missing but can be interprelated from other columns or set with default values. These interprelated values or deafult value will thus fill in those missing values. Here we will show you how to.

### Option 1 - Fill in All Missing Values with a Default Value


```python
(
    pets
    .fillna('Unknown')
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
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>Unknown</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>Unknown</td>
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
      <td>Unknown</td>
      <td>2019-01-01 10:05:10</td>
      <td>13</td>
      <td>Unknown</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

Using `fillna` we attempt to fill in all `Null` values with the value `'Unknown'`. This will be fine if all the `Null` values are strings, but it won't work if for say the `age` column is missing values. We look at how to specify different values for different columns next.

### Option 2 - Fill in All Missing Values with a Mapping


```python
(
    pets
    .fillna({
        'nickname': 'Unknown Nickname',
        'color':    'Unknown Color',
    })
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
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>Unknown Color</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>Unknown Color</td>
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
      <td>Unknown Nickname</td>
      <td>2019-01-01 10:05:10</td>
      <td>13</td>
      <td>Unknown Color</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

You have the option of filling in each column with a diffferent value. This provides more flexibility as most times the columns will be different types and a single deafult value won't be sufficient enough.

### Option 2 - `coalesce()`


```python
(
    pets
    .withColumn('bogus', F.coalesce(F.col('color'), F.col('nickname'), F.lit('Default')))
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
      <th>bogus</th>
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
      <td>Argus</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>Chewie</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
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
      <td>Default</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

Another way to fill in a column with values is using `coalesce()`. This function will try to fill in the specified columns by looking at the given arguments in order from left to right, until one of the arguments is not null and use that. If all else fails, you can provide a "default" value as your last arugment (remembering that it should be a columnar expression). 

In our example, it will attempt to fill in the `bogus` column with values from the `color` column first, if that is null then try the `nickname` column next, and if both are null it will use the deafult value `Default`.

### Summary

* We looked at a generic way of filling in a columns with a single default value.
* We looked at providing a mapping of `{column:value}` to fill in each column seperately.
* Lastly we looked at how to fill in a column using other columns and default values in an order of precedency.
