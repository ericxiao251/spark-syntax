
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
    .appName("Section 2.11  - Unionizing Multiple Dataframes")
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



### Unionizing Multiple Dataframes

There are a couple of situations where you would want to perform an union transformation.

**Case 1: Collecting Data from Various Sources**

When you're collecting data from multiple sources, some point in your spark application you will need to reconcile all the different sources into the same format and work with a single source of truth. This will require you to `union` the different datasets together.

**Case 2: Perfoming Different Transformations on your Dataset**

Sometimes you would like to perform seperate transformations on different parts of your data based on your task. This would involve breaking up your dataset into different parts and working on them individually. Then at some point you might want to stitch they back together, this would again be a `union` operation.

### Case 1 - `union()` (the Wrong Way)


```python
pets_2 = pets.select(
    'breed_id',
    'id',
    'age',
    'color',
    'birthday',
    'nickname'
)

(
    pets
    .union(pets_2)
    .where(F.col('id').isin(1,2))
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
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>5</td>
      <td>brown</td>
      <td>2014-11-22 12:30:31</td>
      <td>King</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>3</td>
      <td>15</td>
      <td>None</td>
      <td>2016-11-22 10:05:10</td>
      <td>Chewie</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>3</td>
      <td>17</td>
      <td>white</td>
      <td>2018-11-22 10:05:10</td>
      <td>Maple</td>
    </tr>
  </tbody>
</table>
</div>



### Case 1 - Another Wrong Way


```python
pets_3 = pets.select(
    '*',
    '*'
)

pets_3.show()

(
    pets
    .union(pets_3)
    .where(F.col('id').isin(1,2))
    .toPandas()
)
```

    +---+--------+--------+-------------------+---+-----+---+--------+--------+-------------------+---+-----+
    | id|breed_id|nickname|           birthday|age|color| id|breed_id|nickname|           birthday|age|color|
    +---+--------+--------+-------------------+---+-----+---+--------+--------+-------------------+---+-----+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|  1|       1|    King|2014-11-22 12:30:31|  5|brown|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|  2|       3|   Argus|2016-11-22 10:05:10| 10| null|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|  3|       2|   Maple|2018-11-22 10:05:10| 17|white|
    +---+--------+--------+-------------------+---+-----+---+--------+--------+-------------------+---+-----+
    



    ---------------------------------------------------------------------------

    AnalysisException                         Traceback (most recent call last)

    <ipython-input-5-c8157f574918> in <module>()
          8 (
          9     pets
    ---> 10     .union(pets_3)
         11     .where(F.col('id').isin(1,2))
         12     .toPandas()


    /usr/local/lib/python2.7/site-packages/pyspark/sql/dataframe.pyc in union(self, other)
       1336         Also as standard in SQL, this function resolves columns by position (not by name).
       1337         """
    -> 1338         return DataFrame(self._jdf.union(other._jdf), self.sql_ctx)
       1339 
       1340     @since(1.3)


    /usr/local/lib/python2.7/site-packages/py4j/java_gateway.pyc in __call__(self, *args)
       1255         answer = self.gateway_client.send_command(command)
       1256         return_value = get_return_value(
    -> 1257             answer, self.gateway_client, self.target_id, self.name)
       1258 
       1259         for temp_arg in temp_args:


    /usr/local/lib/python2.7/site-packages/pyspark/sql/utils.pyc in deco(*a, **kw)
         67                                              e.java_exception.getStackTrace()))
         68             if s.startswith('org.apache.spark.sql.AnalysisException: '):
    ---> 69                 raise AnalysisException(s.split(': ', 1)[1], stackTrace)
         70             if s.startswith('org.apache.spark.sql.catalyst.analysis'):
         71                 raise AnalysisException(s.split(': ', 1)[1], stackTrace)


    AnalysisException: u"Union can only be performed on tables with the same number of columns, but the first table has 6 columns and the second table has 12 columns;;\n'Union\n:- Relation[id#10,breed_id#11,nickname#12,birthday#13,age#14,color#15] csv\n+- Project [id#10, breed_id#11, nickname#12, birthday#13, age#14, color#15, id#10, breed_id#11, nickname#12, birthday#13, age#14, color#15]\n   +- Relation[id#10,breed_id#11,nickname#12,birthday#13,age#14,color#15] csv\n"


**What Happened?**

This actually worked out quite nicely, I forgot this was the case actually. **Spark will only allow you to union `df` that have the exact number of columns and where the column datatypes are exactly the same.**

**Case 1**

Because we infered the schema and datatypes from the csv file it was able to union the 2 dataframes, but the results doesn't make sense at all; The columns don't match up.

**Case 2**

We created a new dataframe with twice the numnber of columns and tried to union it with the original `df`, spark threw an error as it doesn't know what to do when the number of columns don't match up.

### Case 2 - `union()` (the Right Way)


```python
(
    pets
    .union(pets_2.select(pets.columns))
    .union(pets_3.select(pets.columns))
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
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>11</th>
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

The columns match perfectly! How? **For each of the new `df` that you would like to union with the original `df` you will `select` the column from the original `df` during the union.** This will:
1. Guarantees the ordering of the columns, as a `select` will select the columns in order of which they are listed in.
2. Guarantees that only the columns of the original `df` is selected, from the previous sections, we know that `select` will only the specified columns.

### Summary

* Always always be careful when you are `union`ing `df` together.
* When you `union` `df`s together you should ensure:
    1. The number of columns are the same.
    2. The columns are the exact same.
    3. The columns are in the same order.
