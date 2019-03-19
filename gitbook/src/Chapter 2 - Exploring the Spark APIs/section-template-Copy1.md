
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import types as T
```

### Template


```python
spark = SparkSession.builder \
    .master("local") \
    .appName("Section 4 - More Comfortable with SQL?") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

import os

data_path = "/data/pets.csv"
base_path = os.path.dirname(os.getcwd())
path = base_path + data_path

df = spark.read.csv(path, header=True)
df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>species_id</th>
      <th>name</th>
      <th>birthday</th>
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
      <td>brown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>


