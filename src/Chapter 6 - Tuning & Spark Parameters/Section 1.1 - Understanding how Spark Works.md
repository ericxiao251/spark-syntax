# Section 1.1 - Understanding Distributed Systems and how Spark Works

Working with Spark requires a different kind of thinking. Your code isn't executing in a sequential manor anymore, it's being executing in parallel. To write performant parallel code, you will need to think about how you can perform as different/same tasks at the same while minimizing the blocking of other tasks. Hopefully by understanding how spark and distributed systems work you can get into the right mindset and write clean parallel spark code.

## Understanding Distributed Systems/Computing

**Sequential Applications**

In a generic application the code path is run in a sequential order. As in the code will execute from the top of the file to the bottom of the file, line by line.

**Multiprocess/threaded Applications**

In a `multi-processed/threaded` application the code path will diverge. The application will assign a portions of the code to `threads/processes` which will handle these tasks in an `asynchronous` manner. Once these tasks are completed the `threads/processes` will signal the main application and the code path will conform again.

These `threads/processes` are allocated a certain amount of memory and processing power on a single machine where the main application is running.

Think about how your computer can handle multiple applications at once. This is multiple processes running on a single machine.

**Distributed Computing (Clusters and Nodes)**

Spark is a distributed computing library that can be used in either Python, Scala, Java or R. When we say "distributed computing" we mean that our application runs on multiple machines/computers called `Nodes` which are all part of a single `Cluster`.

This is very similar to how a `multi-processed` application would work, just with more processing juice. Each `Node` is essentially a computer running multiple process running at once.

![](https://github.com/ericxiao251/spark-syntax/blob/master/src/images/master-slave.png)

**Master/Slave Architecture**

From the image in the last section we can see there are 2 types of `Nodes`, a single `Driver/Master Node` and multiple `Worker Nodes`.

Each machine is assigned a portion of the overall work in the application. Through communication and message passing the machines attempt to compute each portion of the work in parallel. When the work is dependent on another portion of work, then it will have to wait until that work is computed and passed to the worker. When every portion of the work is done, it is all sent back to the `Master Node`. The coordination of the communication and message passing is done  by the `Master Node` talking to the `Worker Nodes`.

You can think of it as a team lead with multiple engineers, UX, designers, etc. The lead assigns tasks to everyone. The designers and UX collaborate to create an effective interface that is user friendly, once they are done they report back to the lead. The lead will pass this information to engineers and they can start coding the interface, etc.

## Lazy Execution

When you're writing a spark application, no work is actually being done until you perform a `collect` action. As seen in some examples in the previous chapters, a `collect` action is when you want to see the results of your spark transformations in the form of a `toPandas()`, `show()`, etc. This triggers the `Driver` to start the distribution of work, etc.

For now this is all you need to know, we will look into why Spark works this way and why it's a desired design decision.

## MapReduce

When the `Driver Node` actually starts to do some work, it communications and distributes work using a technique called "MapReduce". There are two essential behaviors of a MapReduce application, `map` and `reduce`.

![](https://github.com/ericxiao251/spark-syntax/blob/master/src/images/mapreduce.png)

**Map**

When the `Driver Node` is distributing the work it `maps` the 1) data and 2) transformations to each `Worker Node`. This allows the `Worker Nodes` to perform the work (transformations) on the associated data in parallel.

Ex.

```python
# initialize your variables
x = 5
y = 10

# do some transformations to your variables
x = x * 2
y = y + 2
```

Here we can see that the arithmetic operations performed on `x` and `y` can be done independently, so we can do those 2 operations in parallel on two different `Worker Nodes`. So Spark will `map` `x` and the operation `x * 2` to a `Worker Node` and `y` and `y + 2` to another `Worker Node`.

Think about this but on a larger scale, we have 1 billion rows of numbers that we want to increment by 1. We will map portions of the data to each `Worker Node` and the operation `+ 1` to each `Worker Node`.

**Reduce**

When work can't be done in parallel and is dependent on some previous work, the post transformed `data` is sent back to the `Driver Node` from all the `Worker Nodes`. There the new data may be redistributed to the `Worker Nodes` to resume execution or execution is done on the `Driver Node` depending on the type of work.

Ex.

```python
# initialize your variables
x = 5
y = 10

# do some transformations to your variables
x = x * 2
y = y + 2

# do some more transformations
z = x + y
```

Similar to the example above, but here we see that the last transformation `z = x + y` depends on the previous transformations. So we will need to collect all the work done on the `Worker Nodes` to the `Driver Node` and perform the final transformation.

## Key Terms

**Driver Node**

Learnt above.

**Worker**

Learnt above.

**Executor**

A process launched from an application on a `Worker Node`, that runs tasks and keeps data in memory or disk storage across them. Each application has its own executors.

**Jobs**

Job A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action.

**Stages**

Smaller set of tasks inside any job.

**Tasks**

Unit of work that will be sent to one executor.

[Source](https://www.slideshare.net/DatioBD/apache-spark-ii-sparksql)

![](https://github.com/ericxiao251/spark-syntax/blob/master/src/images/key-terms.png)

[Source](https://www.oreilly.com/library/view/learning-spark/9781449359034/)
