
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
    .appName("Section 1.3 - Maps and Dictionaries")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext

def get_csv_schema(*args):
    return T.StructType([
        T.StructField(*arg)
        for arg in args
    ])

def read_csv(fname, schema):
    return spark.read.csv(
        path=fname,
        header=True,
        schema=get_csv_schema(*schema)
    )

import os

data_path = "/data/pets.csv"
base_path = os.path.dirname(os.getcwd())
path = base_path + data_path
```


```python
pets = spark.read.csv(path, header=True)
pets.show()
```

    +---+--------+--------+-------------------+---+-----+------+
    | id|breed_id|nickname|           birthday|age|color|weight|
    +---+--------+--------+-------------------+---+-----+------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|  10.0|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|   5.5|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|    12|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|   3.4|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null|    10|
    +---+--------+--------+-------------------+---+-----+------+
    


### Maps and Dictionaries


### Case 1: Creating a Mapping from Existing Columns


```python
(
    pets
    .fillna({
        'nickname': 'Unknown Name',
        'age':      'Unknown Age',
    })
    .withColumn('{nickname:age}', F.create_map(F.col('nickname'), F.col('age')))
    .withColumn('{nickname:age} 2', F.create_map('nickname', 'age'))
    .show()
)
```

    +---+--------+------------+-------------------+---+-----+------+--------------------+--------------------+
    | id|breed_id|    nickname|           birthday|age|color|weight|      {nickname:age}|    {nickname:age} 2|
    +---+--------+------------+-------------------+---+-----+------+--------------------+--------------------+
    |  1|       1|        King|2014-11-22 12:30:31|  5|brown|  10.0|         [King -> 5]|         [King -> 5]|
    |  2|       3|       Argus|2016-11-22 10:05:10| 10| null|   5.5|       [Argus -> 10]|       [Argus -> 10]|
    |  3|       1|      Chewie|2016-11-22 10:05:10| 15| null|    12|      [Chewie -> 15]|      [Chewie -> 15]|
    |  3|       2|       Maple|2018-11-22 10:05:10| 17|white|   3.4|       [Maple -> 17]|       [Maple -> 17]|
    |  4|       2|Unknown Name|2019-01-01 10:05:10| 13| null|    10|[Unknown Name -> 13]|[Unknown Name -> 13]|
    +---+--------+------------+-------------------+---+-----+------+--------------------+--------------------+
    


**What Happened?**

You can create a column of map types using either `columnary expressions` (we'll learn what column expressions are later) or column names.

### Case 2: Creating a Mapping from Constant Values


```python
(
    pets
    .fillna({
        'nickname': 'Unknown Name',
        'age':      'Unknown Age',
    })
    .withColumn('{nickname:age}', F.create_map(F.lit('key'), F.lit('value')))
    .show()
)
```

    +---+--------+------------+-------------------+---+-----+------+--------------+
    | id|breed_id|    nickname|           birthday|age|color|weight|{nickname:age}|
    +---+--------+------------+-------------------+---+-----+------+--------------+
    |  1|       1|        King|2014-11-22 12:30:31|  5|brown|  10.0|[key -> value]|
    |  2|       3|       Argus|2016-11-22 10:05:10| 10| null|   5.5|[key -> value]|
    |  3|       1|      Chewie|2016-11-22 10:05:10| 15| null|    12|[key -> value]|
    |  3|       2|       Maple|2018-11-22 10:05:10| 17|white|   3.4|[key -> value]|
    |  4|       2|Unknown Name|2019-01-01 10:05:10| 13| null|    10|[key -> value]|
    +---+--------+------------+-------------------+---+-----+------+--------------+
    


**What Happened?**

You can create a column of map types of literals using the `columnary expression` `F.lit()`, we will learn this later on. Notice that each key/value needs to be a `columnal expression`? This will be a common theme throughout Spark.

### Summary

* It is very simple to create map data in Spark.
* You can do so with both existing columns or constant values.
* If constant values are used, then each value must be a `columnary expression`.
