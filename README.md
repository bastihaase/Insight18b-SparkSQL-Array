# Array modifications in SparkSQL





## One sentence description
I implemented array modifications internally with SparkSQL and compare performance to UDFs. See here for my modified Spark Distro: [modified-Spark](https://github.com/bastihaase/spark)

This repo contains the code that was used to build the pipeline and do the performance testing.


## Details and use-case

Imagine you are batch-processing or streaming purchase data from a website like Amazon.
With every purchase, you have a JSON object that also contains arrays of other objects
that were purchased and of objects that the buyer viewed. We want to understand
how many elements these two arrays have in common. To do this with SparkSQL,
we currently need to use UDFs. I will implement this internally and then
measure performance of both methods.

- stream/batch-process metadata and store them ordered by number of intersections in MySQL
- store the metadata in HDFS
- output a dashboard that shows statistics


The dataflow can be described as follows:

![image](images/tech.png "Tech-stack")


## Dataset

The dataset will be Amazon purchase metadata.

## Technologies chosen/considered

For batch processing, I will use HDFS to SparkSQL to MySQL to Flask.
For reliably performance testing, I want to make sure that the processing in
SparkSQL is the bottleneck of the pipeline.

* Our data is relational, so we choose MySQL
* The queries we want to display are of the form SELECT COUNT(*) FROM table GROUP BY overlap where overlap is the size of the intersection

We will use HDFS for storage as it is well-maintained with Spark.
Lastly we will use Flask to visualize results.
