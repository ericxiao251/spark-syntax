
# Error Log #1
```
Stdoutput Caused by: 
org.apache.spark.SparkException: Job aborted due to stage failure: ShuffleMapStage 32 (parquet at NativeMethodAccessorImpl.java:0) has failed the maximum allowable number of times: 4. 
...
Most recent failure reason: org.apache.spark.shuffle.FetchFailedException: Too large frame: 4410995563
```
### What does this mean?
* When performing a `join` on multiple `DataFrame`s, data is usually shuffled in smaller chunks called `partitions`. 
* This errors indicates that the **partitions shuffled are too large**.

### Solution
* The default partition size is `200`. 
* Try to playing around with the `spark.sql.shuffle.partitions` parameter, use values that are a power of 2, ie. 2^11 = `2048`.
* Increasing the number of partitions will decrease the size of each partition.

# Error Log #2
```
Stdoutput py4j.protocol.Py4JJavaError: An error occurred while calling o3092.freqItems.
Stdoutput : org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 43334 tasks (5.4 GB) is bigger than spark.driver.maxResultSize (5.4 GB)
```
### What does this mean?
* The amount of data that you are pulling back to the driver to is large!
* This is the result of performing some sort of `collect` which brings all the data to one processor, the driver.

### Solution
* If this is really what you want, then increasing the driver's heap might help.
* Alternately if this isn't what you want, try instead of a `collect` use a `head`, `take`, etc. This will only take a collect a couple of rows to the driver.


```python

```
