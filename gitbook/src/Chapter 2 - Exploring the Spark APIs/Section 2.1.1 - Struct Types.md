
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
    .appName("Section 1.1 - Struct Types")
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
    


### Struct Types

What are they used for? TODO


```python
(
    pets
    .withColumn('struct_col', F.struct('nickname', 'birthday', 'age', 'color'))
    .withColumn('nickname_from_struct', F.col('struct_col').nickname)
    .show()
)
```

    +---+--------+--------+-------------------+---+-----+------+--------------------+--------------------+
    | id|breed_id|nickname|           birthday|age|color|weight|          struct_col|nickname_from_struct|
    +---+--------+--------+-------------------+---+-----+------+--------------------+--------------------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|  10.0|[King, 2014-11-22...|                King|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|   5.5|[Argus, 2016-11-2...|               Argus|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|    12|[Chewie, 2016-11-...|              Chewie|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|   3.4|[Maple, 2018-11-2...|               Maple|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null|    10|[, 2019-01-01 10:...|                null|
    +---+--------+--------+-------------------+---+-----+------+--------------------+--------------------+
    


**What Happened?**

We created a `struct` type column consisting of the columns `'nickname', 'birthday', 'age', 'color'`. Then we accessed a member `nickname` from the struct.

### Summary

* TODO: Fix a use-case.
* It is pretty easy creating and accessing `struct` datatypes.
