<<<<<<< HEAD
# Intro
Camus is LinkedIn's [Kafka](http://kafka.apache.org "Kafka")->HDFS pipeline. It is a mapreduce job that does distributed data loads out of Kafka. It includes the following features:

* Automatic discovery of topics
* Avro schema management / In progress
* Date partitioning

It is used at LinkedIn where it processes tens of billions of messages per day. You can get a basic overview from this paper: [Building LinkedIn’s Real-time Activity Data Pipeline](http://sites.computer.org/debull/A12june/pipeline.pdf "Building LinkedIn’s Real-time Activity Data Pipeline").

There is a [Google Groups mailing list](https://groups.google.com/forum/#!forum/camus_etl "Google Groups mailing list") that you can email or search if you have any questions.

For a more detailed documentation on the main Camus components, please see [Camus InputFormat and OutputFormat Behavior](https://github.com/linkedin/camus/wiki/Camus-InputFormat-and-OutputFormat-Behavior "Camus InputFormat and OutputFormat Behavior")
# Brief Overview
All work is done within a single Hadoop job divided into three stages:

1. Setup stage fetches available topics and partitions from Zookeeper and the latest offsets from the Kafka Nodes.

1. Hadoop job stage allocates topic pulls among a set number of tasks.  Each task does the following:
    *  Fetch events from Kafka server and collect count statistics.
    *  Move data files to corresponding directories based on timestamps of events.
    *  Produce count  events and write to HDFS.  * TODO: Determine status of open sourcing Kafka Audit.
    *  Store updated offsets in HDFS.

1. Cleanup stage reads counts from all tasks, aggregates the values, and submits the results to Kafka for consumption by Kafka Audit. 

## Setup Stage 

1. Setup stage fetches from Zookeeper Kafka broker urls and topics (in /brokers/id and /brokers/topics).  This data is transient and will be gone once Kafka server is down.

1. Topic offsets stored in HDFS.  Camus maintains its own status by storing offset for each topic in HDFS. This data is persistent.

1. Setup stage allocates all topics and partitions among a fixed number of tasks.

## Hadoop Stage 

### 1. Pulling the Data 

Each hadoop task uses a list of topic partitions with offsets generated by setup stage as input. It uses them to initialize Kafka requests and fetch events from Kafka brokers. Each task generates four types of outputs (by using a custom MultipleOutputFormat):
Avro data files,
Count statistics files,
Updated offset files,
and Error files. 

* Note, each task generates an error file even if no errors were encountered.  If no errors occurred, the file is empty.

### 2. Committing the data 

Once a task has successfully completed, all topics pulled are committed to their final output directories. If a task doesn't complete successfully, then none of the output is committed.  This allows the hadoop job to use speculative execution.  Speculative execution happens when a task appears to be running slowly.  In that case the job tracker then schedules the task on a different node and runs both the main task and the speculative task in parallel.  Once one of the tasks completes, the other task is killed.  This prevents a single overloaded hadoop node from slowing down the entire ETL.

### 3. Producing Audit Counts 

Successful tasks also write audit counts to HDFS. 

### 4. Storing the Offsets 

Final offsets are written to HDFS and consumed by the subsequent job.

## Job Cleanup 

Once the hadoop job has completed, the main client reads all the written audit counts and aggregates them.  The aggregated results are then submitted to Kafka.

## Setting up Camus

### Building the project

You can build Camus with:
```
mvn clean package
```

Note that there are two jars that are not currently in a public Maven repo. These jars (kafka-0.7.2 and avro-schema-repository-1.74-SNAPSHOT) are supplied in the lib directory, and maven will automatically install them into your local Maven cache (usually ~/.m2).

### First, Create a Custom Kafka Message to Avro Record Decoder

We hope to eventually create a more out of the box solution, but until we get there you will need to create a custom decoder for handling Kafka messages.  You can do this by implementing the abstract class com.linkedin.batch.etl.kafka.coders.KafkaMessageDecoder.  Internally, we use a schema registry that enables obtaining an Avro schema using an identifier included in the Kafka byte payload. For more information on other options, you can email camus_etl@googlegroups.com.  Once you have created a decoder, you will need to specify that decoder in the properties as described below.  You can also start by taking a look at the existing Decoders to see if they will work for you, or as examples if you need to implement a new one.  

### Decoding JSON messages

Camus can also process JSON messages. Set "camus.message.decoder.class=com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder" in camus.properties. Additionally, there are two more options "camus.message.timestamp.format" (default value: "[dd/MMM/yyyy:HH:mm:ss Z]") and "camus.message.timestamp.field" (default value: "timestamp").

### Writing to different formats

By default Camus writes Avro data.  But you can also write to different formats by implementing and RecordWriterProvider.  For examples see https://github.com/linkedin/camus/blob/master/camus-etl-kafka/src/main/java/com/linkedin/camus/etl/kafka/common/AvroRecordWriterProvider.java and https://github.com/linkedin/camus/blob/master/camus-etl-kafka/src/main/java/com/linkedin/camus/etl/kafka/common/StringRecordWriterProvider.java.  You can specify which writer to use with "etl.record.writer.provider.class".

### Configuration

Camus can be run from the command line as Java App.  You will need to set some properties either by specifying a properties file on the classpath using -p (filename), or an external properties file using -P (filepath to local file system, or to `hdfs:`), or from the command line itself using -D property=value. If the same property is set using more than one of the previously mentioned methods, the order of precedence is command-line, external file, classpath file.

Here is an abbreviated list of commonly used parameters.  An example properties file is also located https://github.com/linkedin/camus/blob/master/camus-example/src/main/resources/camus.properties.

* Top-level data output directory, sub-directories will be dynamically created for each topic pulled
    * `etl.destination.path=`
* HDFS location where you want to keep execution files, i.e. offsets, error logs, and count files
    * `etl.execution.base.path=`
* Where completed Camus job output directories are kept, usually a sub-dir in the base.path
    * `etl.execution.history.path=`
* Filesystem for the above folders. This can be a hdfs:// or s3:// address.
    * `fs.default.name=`
* List of at least one Kafka broker for Camus to pull metadata from
    * `kafka.brokers=`    
* All files in this dir will be added to the distributed cache and placed on the classpath for Hadoop tasks
    * `hdfs.default.classpath.dir=`
* Max hadoop tasks to use, each task can pull multiple topic partitions
    * `mapred.map.tasks=30`
* Max historical time that will be pulled from each partition based on event timestamp
    * `kafka.max.pull.hrs=1`
* Events with a timestamp older than this will be discarded. 
    * `kafka.max.historical.days=3`
* Max minutes for each mapper to pull messages
    * `kafka.max.pull.minutes.per.task=-1`
* Decoder class for Kafka Messages to Avro Records
    * `camus.message.decoder.class=`
* If whitelist has values, only whitelisted topic are pulled.  Nothing on the blacklist is pulled
    * `kafka.blacklist.topics=`
    * `kafka.whitelist.topics=`
* Class for writing records to HDFS/S3
    * `etl.record.writer.provider.class=`
* Delimiter for writing string records (default is "\n")
    * `etl.output.record.delimiter=`

### Running Camus

Camus can be run from the command line using hadoop jar.  Here is the usage:
```
usage: hadoop jar camus-example-<version>-SNAPSHOT.jar com.linkedin.camus.etl.kafka.CamusJob  <br/>
 -D <property=value>   use value for given property<br/>
 -P <arg>              external properties filename<br/>
 -p <arg>              properties filename from the classpath<br/>
```
=======
# camus
>>>>>>> 0070a771bf334b77c9d42194b186263de50f44da
