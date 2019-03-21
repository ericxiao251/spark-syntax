
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
    .appName("Section 2.1.2 - Arrays and Lists")
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
pets = read_csv(
    fname=path,
    schema=[
        ("id", T.LongType(), False),
        ("breed_id", T.LongType(), True),
        ("nickname", T.StringType(), True),
        ("birthday", T.TimestampType(), True),
        ("age", T.LongType(), True),
        ("color", T.StringType(), True),
        ("weight", T.DecimalType(), True),
    ]
)
pets.show()
```

    +---+--------+--------+-------------------+---+-----+------+
    | id|breed_id|nickname|           birthday|age|color|weight|
    +---+--------+--------+-------------------+---+-----+------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|    10|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|     6|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|    12|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|     3|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null|    10|
    +---+--------+--------+-------------------+---+-----+------+
    


### Arrays and Lists

### Case 1: Reading in Data that contains `Arrays`

TODO

### Case 2: Creating Arrays


```python
(
    pets
    .withColumn('array column', F.array([
        F.lit(1),
        F.lit("Bob"),
        F.lit(datetime(2019,2,1)),
    ]))
    .show()
)
```

    +---+--------+--------+-------------------+---+-----+------+--------------------+
    | id|breed_id|nickname|           birthday|age|color|weight|        array column|
    +---+--------+--------+-------------------+---+-----+------+--------------------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|    10|[1, Bob, 2019-02-...|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|     6|[1, Bob, 2019-02-...|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|    12|[1, Bob, 2019-02-...|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|     3|[1, Bob, 2019-02-...|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null|    10|[1, Bob, 2019-02-...|
    +---+--------+--------+-------------------+---+-----+------+--------------------+
    


**What Happened?**

We will explain in the later chapter what the `F.lit()` function does, but for now understand that in order to create an array type you need to call the `F.array()` function and for each array element call `F.lit()` on.

### Summary

* It's pretty simple to create an array in Spark, you will need to call 2 functions: `F.array()` and `F.lit()`.
* Each element of the array needs to be of type `F.lit()`
