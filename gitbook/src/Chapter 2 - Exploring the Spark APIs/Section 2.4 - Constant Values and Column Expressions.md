
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datetime import date
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Section 2.4 - Constant Values")
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



### Constant Values

There are many instances where you will need to create a `column` expression or use a constant value to perform some of the spark transformations. We'll explore some of these.

### Case 1: Creating a Column with a constant value (`withColumn()`) (wrong)


```python
pets.withColumn('todays_date', date.today()).toPandas()
```


    ---------------------------------------------------------------------------

    AssertionError                            Traceback (most recent call last)

    <ipython-input-4-f87e239cb534> in <module>()
    ----> 1 pets.withColumn('todays_date', date.today()).toPandas()
    

    /usr/local/lib/python2.7/site-packages/pyspark/sql/dataframe.pyc in withColumn(self, colName, col)
       1846 
       1847         """
    -> 1848         assert isinstance(col, Column), "col should be Column"
       1849         return DataFrame(self._jdf.withColumn(colName, col._jc), self.sql_ctx)
       1850 


    AssertionError: col should be Column


**What Happened?**

Spark functions that have a `col` as an argument will usually require you to pass in a `Column` expression. As seen in the previous section, `withColumn()` worked fine when we gave it a column from the current `df`. But this isn't the case when we want set a column to a constant value.

If you get an ```AssertionError: col should be Column``` that is usually the case, we'll look into how to fix this.

### Case 1: Creating a Column with a constant value (`withColumn()`) (correct)


```python
pets.withColumn('todays_date', F.lit(date.today())).toPandas()
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
      <th>todays_date</th>
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
      <td>2019-02-14</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
      <td>2019-02-14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>2019-02-14</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

With `F.lit()` you can create a `column` expression that you can now assign to a new column in your dataframe.

### More Examples


```python
(
    pets
    .withColumn('age_greater_than_5', F.col("age") > 5)
    .withColumn('height', F.lit(150))
    .where(F.col('breed_id') == 1)
    .where(F.col('breed_id') == F.lit(1))
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
      <th>age_greater_than_5</th>
      <th>height</th>
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
      <td>False</td>
      <td>150</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>True</td>
      <td>150</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

(We will look into equilities statements later.)

The above contains constant values (column `height`) and column expressions (columns using `F.col()`) so a `F.lit()` is not required.

### Summary

* You need to use `F.lit()` to assign constant values to columns.
* Equality expressions with `F.col()` is also another way to have a column expressions.
* When in doubt, always use column expressions `F.lit()`.
