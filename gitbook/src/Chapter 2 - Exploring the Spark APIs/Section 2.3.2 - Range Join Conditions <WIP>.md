
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Section 3.2 - Range Join Conditions (WIP)")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)


sc = spark.sparkContext
```


```python
geo_loc_table = spark.createDataFrame([
    (1, 10, "foo"), 
    (11, 36, "bar"), 
    (37, 59, "baz"),
], ["ipstart", "ipend", "loc"])

geo_loc_table.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ipstart</th>
      <th>ipend</th>
      <th>loc</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>10</td>
      <td>foo</td>
    </tr>
    <tr>
      <th>1</th>
      <td>11</td>
      <td>36</td>
      <td>bar</td>
    </tr>
    <tr>
      <th>2</th>
      <td>37</td>
      <td>59</td>
      <td>baz</td>
    </tr>
  </tbody>
</table>
</div>




```python
records_table = spark.createDataFrame([
    (1, 11), 
    (2, 38), 
    (3, 50),
],["id", "inet"])

records_table.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>inet</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>11</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>38</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>50</td>
    </tr>
  </tbody>
</table>
</div>



### Range Join Conditions

> A naive approach (just specifying this as the range condition) would result in a full cartesian product and a filter that enforces the condition (tested using Spark 2.0). This has a horrible effect on performance, especially if DataFrames are more than a few hundred thousands records.

source: http://zachmoshe.com/2016/09/26/efficient-range-joins-with-spark.html

> The source of the problem is pretty simple. When you execute join and join condition is not equality based the only thing that Spark can do right now is expand it to Cartesian product followed by filter what is pretty much what happens inside `BroadcastNestedLoopJoin`

source: https://stackoverflow.com/questions/37953830/spark-sql-performance-join-on-value-between-min-and-max?answertab=active#tab-top

### Option #1


```python
join_condition = [
    records_table['inet'] >= geo_loc_table['ipstart'],
    records_table['inet'] <= geo_loc_table['ipend'],
]

df = records_table.join(geo_loc_table, join_condition, "left")

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>inet</th>
      <th>ipstart</th>
      <th>ipend</th>
      <th>loc</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>11</td>
      <td>11</td>
      <td>36</td>
      <td>bar</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>38</td>
      <td>37</td>
      <td>59</td>
      <td>baz</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>50</td>
      <td>37</td>
      <td>59</td>
      <td>baz</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    BroadcastNestedLoopJoin BuildRight, LeftOuter, ((inet#252L >= ipstart#245L) && (inet#252L <= ipend#246L))
    :- Scan ExistingRDD[id#251L,inet#252L]
    +- BroadcastExchange IdentityBroadcastMode
       +- Scan ExistingRDD[ipstart#245L,ipend#246L,loc#247]


### Option #2


```python
from bisect import bisect_right
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

geo_start_bd = spark.sparkContext.broadcast(map(lambda x: x.ipstart, geo_loc_table
    .select("ipstart")
    .orderBy("ipstart")
    .collect()
))

def find_le(x):
    'Find rightmost value less than or equal to x'
    i = bisect_right(geo_start_bd.value, x)
    if i:
        return geo_start_bd.value[i-1]
    return None

records_table_with_ipstart = records_table.withColumn(
    "ipstart", udf(find_le, LongType())("inet")
)

df = records_table_with_ipstart.join(geo_loc_table, ["ipstart"], "left")

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ipstart</th>
      <th>id</th>
      <th>inet</th>
      <th>ipend</th>
      <th>loc</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>37</td>
      <td>2</td>
      <td>38</td>
      <td>59</td>
      <td>baz</td>
    </tr>
    <tr>
      <th>1</th>
      <td>37</td>
      <td>3</td>
      <td>50</td>
      <td>59</td>
      <td>baz</td>
    </tr>
    <tr>
      <th>2</th>
      <td>11</td>
      <td>1</td>
      <td>11</td>
      <td>36</td>
      <td>bar</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(4) Project [ipstart#272L, id#251L, inet#252L, ipend#246L, loc#247]
    +- SortMergeJoin [ipstart#272L], [ipstart#245L], LeftOuter
       :- *(2) Sort [ipstart#272L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(ipstart#272L, 200)
       :     +- *(1) Project [id#251L, inet#252L, pythonUDF0#281L AS ipstart#272L]
       :        +- BatchEvalPython [find_le(inet#252L)], [id#251L, inet#252L, pythonUDF0#281L]
       :           +- Scan ExistingRDD[id#251L,inet#252L]
       +- *(3) Sort [ipstart#245L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(ipstart#245L, 200)
             +- Scan ExistingRDD[ipstart#245L,ipend#246L,loc#247]

