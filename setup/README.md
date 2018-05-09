# Setup of technologies

We will quickly run through how to set up the technologies and install the frameworks that were used in this project.


## Cluster

I used (Pegasus)[https://github.com/InsightDataScience/pegasus] to setup my technologies.
After a succesful installation/initialization of Pegasus, run the following commands to setup your AWS cluster :

    peg up <cluster-name>

where you specified the topology of your cluster in a yaml file.

Before installing technologies, we need to setup ssh and aws access via

    peg install <cluster-name> ssh
    peg install <cluster-name> aws

We can now start installing technologies, we start with hadoop:

    peg install <cluster-name> hadoop
    peg service <cluster-name> hadoop start

When installing Spark, be sure to employ my custom version to use my added
functionality (It is based on a 2.4.0 snapshot). You can either compile my
version from scratch (Link)[https://github.com/bastihaase/spark] or, as long as
Insight keeps up my AWS cluster, use the link
[https://s3-us-west-2.amazonaws.com/bastian-haase-insight-18b/spark-2.4.0-SNAPSHOT-bin-my-spark.tgz](https://s3-us-west-2.amazonaws.com/bastian-haase-insight-18b/spark-2.4.0-SNAPSHOT-bin-my-spark.tgz).
If you compile it yourself, see here for instructions (Compile Spark)[https://spark.apache.org/docs/2.3.0/building-spark.html].
Note that I encountered errors with the compilation of SparkR due to a knitr error.
Make sure you either do not include SparkR in your build or have
knitr installed properly.
Make sure that you update /pegasus/install/technology_download with a
link to this spark distribution. (You should generally check if
the download links are up to date as they frequently change).

Then, we can set up Spark as follows:

    peg install <cluster-name> spark
    peg service <cluster-name> spark start

For streaming, we als need to install Zookeeper and Kafka.

    peg install <cluster-name> zookeeper
    peg service <cluster-name> zookeeper start
    peg install <cluster-name> kafka
    peg service <cluster-name> kafka start

We also need to setup a node for MySQL. After setting up a node with Pegasus, I
just installed MySQL via apt-get. Then, go ahead and create tables name *output*
for batch and *streaming* for streaming.

For the table *output*, use the following schema:

    asin VARCHAR(255), description VARCHAR(20), price FLOAT, overlap INT

For streaming, use:

    asin VARCHAR(255), overlap INT

In both cases, make sure to make overlap indexed for efficient querying.

## Data

The Amazon purchase data that I used was from this website: [Data](http://jmcauley.ucsd.edu/data/amazon/links.html).
This data should be available on HDFS so that you can submit a relative path name with the spark jobs.


## Spark Jobs

All Spark jobs can be found in this repo together with build and assembly files that details
which additional jars are needed. Note that we have often toggled the *provided* option to
avoid fat jars and because we often need dependencies that are not in the official maven
repositories yet (as we are using Spark 2.4.0). So, when submitting the Spark jobs, be sure
to include these jars.

A typical submit for a batch job (where the data was named original.json and is
lying in a hadoop folder named input):

    spark-submit --master spark://IP-address:7077 --class Performance_Tests.SparkSQL_Performance  performance.jar  metadata_big.json performance
 This will run the SparkSQL_Performance job using the internal solution.
 To run the UDF version, use

     spark-submit --master spark://IP-address:7077 --class Performance_Tests.SparkSQL_Performance  performance.jar  metadata_big.json UDF

For a streaming job, use

     spark-submit --master spark://IP-address:7077 --class Kafka_SQL.Streaming --jars kafka.jar,mysql-connector-java-5.1.38.jar streaming.jar performance

Here Kafka.jar is a Kafka-Spark connector jar for Spark 2.4.0 and the mysql-connector jar should be included
in all jobs that write to MySQL.


## Flask

The installation of the flask app is just a straightforward cloning of all files in the app folder.
I used (Gunicorn)[http://gunicorn.org/] to deploy the web app.
