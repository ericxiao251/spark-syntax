
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
    .appName("Section 2.12 - Performing Joins (clean one)")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext
```


```python
pets = spark.createDataFrame(
    [
        (1, 1, 'Bear'),
        (2, 1, 'Chewie'),
        (3, 2, 'Roger'),
    ], ['id', 'breed_id', 'nickname']
)

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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>Roger</td>
    </tr>
  </tbody>
</table>
</div>




```python
breeds = spark.createDataFrame(
    [
        (1, 'Pitbull', 10), 
        (2, 'Corgie', 20), 
    ], ['id', 'name', 'average_height']
)

breeds.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Corgie</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



### Performing Joins
There are typically two types of joins in sql:
1. `Inner Join` is where 2 tables are joined on the basis of common columns mentioned in the ON clause.

    ie. `left.join(right, left[lkey] == right[rkey])`
    

2. `Natural Join` is where 2 tables are joined on the basis of all common columns.      

    ie. `left.join(right, 'key')`

source: https://stackoverflow.com/a/8696402

**Question:**
    Which is better? Is it just a style choice?

### Option 1: Inner Join (w/Different Keys)


```python
join_condition = pets['breed_id'] == breeds['id']

df = pets.join(breeds, join_condition)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>id</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>1</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
      <td>1</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>Roger</td>
      <td>2</td>
      <td>Corgie</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened**:
* We have 2 columns named `id`, but they refer to different things.
* We can't uniquely reference these 2 columns (easily, still possible).
* Pretty long `join expression`.

This is not ideal. Let's try `renaming` it before the join?

### Option 2: Inner Join (w/Same Keys)


```python
breeds = breeds.withColumnRenamed('id', 'breed_id')
join_condition = pets['breed_id'] == breeds['breed_id']

df = pets.join(breeds, join_condition)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>breed_id</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>1</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
      <td>1</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>Roger</td>
      <td>2</td>
      <td>Corgie</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened**:
* We have 2 columns named `breed_id` which mean the same thing!
* Duplicate columns appear in the result.
* Still pretty long `join expression`.

This is again not ideal.

### Option 3: Natural Join


```python
df = pets.join(breeds, 'breed_id')
df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>id</th>
      <th>nickname</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>Chewie</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>3</td>
      <td>Roger</td>
      <td>Corgie</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened**:
* No duplicated column!
* No extra column!
* A single string required for the `join expression` (list of column/keys, if joining on multiple column/keys join).

### Summary

Preforming a `natural join` was the most elegant solution in terms of `join expression` and the resulting `df`.

**NOTE:** These rules also apply to the other join types (ie. `left` and `right`).

**Some might argue that you will need both join keys in the result for further transformations such as filter only the left or right key, but I would recommend doing this before the join, as this is more performant.
