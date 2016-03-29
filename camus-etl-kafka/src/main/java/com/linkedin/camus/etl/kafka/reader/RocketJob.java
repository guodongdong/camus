package com.linkedin.camus.etl.kafka.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.Source;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMapper;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlRecordReader;
import com.linkedin.camus.etl.kafka.reporter.BaseReporter;

public class RocketJob extends Configured implements Tool {

	  public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
	  public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";
	  public static final String ETL_COUNTS_PATH = "etl.counts.path";
	  public static final String ETL_COUNTS_CLASS = "etl.counts.class";
	  public static final String ETL_COUNTS_CLASS_DEFAULT = "com.linkedin.camus.etl.kafka.common.EtlCounts";
	  public static final String ETL_KEEP_COUNT_FILES = "etl.keep.count.files";
	  public static final String ETL_BASEDIR_QUOTA_OVERIDE = "etl.basedir.quota.overide";
	  public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
	  public static final String ETL_FAIL_ON_ERRORS = "etl.fail.on.errors";
	  public static final String ETL_FAIL_ON_OFFSET_OUTOFRANGE = "etl.fail.on.offset.outofrange";
	  public static final String ETL_FAIL_ON_OFFSET_OUTOFRANGE_DEFAULT = Boolean.TRUE.toString();
	  public static final String ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND = "etl.max.percent.skipped.schemanotfound";
	  public static final String ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND_DEFAULT = "0.1";
	  public static final String ETL_MAX_PERCENT_SKIPPED_OTHER = "etl.max.percent.skipped.other";
	  public static final String ETL_MAX_PERCENT_SKIPPED_OTHER_DEFAULT = "0.1";
	  public static final String ZK_AUDIT_HOSTS = "zookeeper.audit.hosts";
	  public static final String KAFKA_MONITOR_TIER = "kafka.monitor.tier";
	  public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
	  public static final String BROKER_URI_FILE = "brokers.uri";
	  public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
	  public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
	  public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
	  public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";
	  public static final String ROCKET_CLIENT_NAME = "rocket.client.name";
	  public static final String ROCKET_PULL_TIME_OUT_MILLIS = "rocket.pull.time.out.millis";
	  public static final String ROCKET_BROKER_SUSPEND_MAX_TIME_MILLIS = "rocket.broker.suspend.max.time.millis";
	  
	  public static final String ROCKET_FETCH_BUFFER_SIZE = "rocket.fetch.buffer.size";
	  public static final String ROCKET_NAMESRVADDR = "names.server.addrs";
	  
	  public static final String ROCKET_HOST_URL = "rocket.host.url";
	  public static final String ROCKET_HOST_PORT = "rocket.host.port";
	  public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
	  public static final String CAMUS_REPORTER_CLASS = "etl.reporter.class";
	  public static final String LOG4J_CONFIGURATION = "log4j.configuration";

	  private static org.apache.log4j.Logger log;
	  private Job hadoopJob = null;

	  private final Properties props;

	  public RocketJob() throws IOException {
	    this(new Properties());
	  }

	  public RocketJob(Properties props) throws IOException {
	    this(props, org.apache.log4j.Logger.getLogger(RocketJob.class));
	  }

	  public RocketJob(Properties props, Logger log) throws IOException {
	    this.props = props;
	    this.log = log;
	  }

	  private static HashMap<String, Long> timingMap = new HashMap<String, Long>();

	  public static void startTiming(String name) {
	    timingMap.put(name, (timingMap.get(name) == null ? 0 : timingMap.get(name)) - System.currentTimeMillis());
	  }

	  public static void stopTiming(String name) {
	    timingMap.put(name, (timingMap.get(name) == null ? 0 : timingMap.get(name)) + System.currentTimeMillis());
	  }

	  public static void setTime(String name) {
	    timingMap.put(name, (timingMap.get(name) == null ? 0 : timingMap.get(name)) + System.currentTimeMillis());
	  }

	  private Job createJob(Properties props) throws IOException {
	    Job job;
	    if (getConf() == null) {
	      setConf(new Configuration());
	    }

	    populateConf(props, getConf(), log);

	    job = new Job(getConf());
	    job.setJarByClass(RocketJob.class);
	    
	    if (job.getConfiguration().get("camus.job.name") != null) {
	      job.setJobName(job.getConfiguration().get("camus.job.name"));
	    } else {
	      job.setJobName("Rocket Job");
	    }

	    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
	      job.getConfiguration().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
	    }

	    this.hadoopJob = job;
	    return job;
	  }

	  public static void populateConf(Properties props, Configuration conf, Logger log) throws IOException {
	    for (Object key : props.keySet()) {
	      conf.set(key.toString(), props.getProperty(key.toString()));
	    }

	    FileSystem fs = FileSystem.get(conf);

	    String hadoopCacheJarDir = conf.get("hdfs.default.classpath.dir", null);

	    List<Pattern> jarFilterString = new ArrayList<Pattern>();

	    for (String str : Arrays.asList(conf.getStrings("cache.jar.filter.list", new String[0]))) {
	      jarFilterString.add(Pattern.compile(str));
	    }

	    if (hadoopCacheJarDir != null) {
	      FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

	      if (status != null) {
	        for (int i = 0; i < status.length; ++i) {
	          if (!status[i].isDir()) {
	            log.info("Adding Jar to Distributed Cache Archive File:" + status[i].getPath());
	            boolean filterMatch = false;
	            for (Pattern p : jarFilterString) {
	              if (p.matcher(status[i].getPath().getName()).matches()) {
	                filterMatch = true;
	                break;
	              }
	            }

	            if (!filterMatch)
	              DistributedCache.addFileToClassPath(status[i].getPath(), conf, fs);
	          }
	        }
	      } else {
	        System.out.println("hdfs.default.classpath.dir " + hadoopCacheJarDir + " is empty.");
	      }
	    }

	    // Adds External jars to hadoop classpath
	    String externalJarList = conf.get("hadoop.external.jarFiles", null);
	    if (externalJarList != null) {
	      String[] jarFiles = externalJarList.split(",");
	      for (String jarFile : jarFiles) {
	        log.info("Adding external jar File:" + jarFile);
	        boolean filterMatch = false;
	        for (Pattern p : jarFilterString) {
	          if (p.matcher(new Path(jarFile).getName()).matches()) {
	            filterMatch = true;
	            break;
	          }
	        }

	        if (!filterMatch)
	          DistributedCache.addFileToClassPath(new Path(jarFile), conf, fs);
	      }
	    }
	  }

	  public void run() throws Exception {
	    run(RocketInputFormat.class, EtlMultiOutputFormat.class);
	  }
	  
	  public void run(Class<? extends InputFormat> inputFormatClass,
	                  Class<? extends OutputFormat> outputFormatClass) throws Exception {
//	    DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss", DateTimeZone.UTC);
	    startTiming("pre-setup");
	    startTiming("total");
	    Job job = createJob(props);
	    if (getLog4jConfigure(job)) {
	      DOMConfigurator.configure("log4j.xml");
	    }
	    FileSystem fs = FileSystem.get(job.getConfiguration());

	    log.info("Dir Destination set to: " + EtlMultiOutputFormat.getDestinationPath(job));

	    Path execBasePath = new Path(props.getProperty(ETL_EXECUTION_BASE_PATH));
	    Path execHistory = new Path(props.getProperty(ETL_EXECUTION_HISTORY_PATH));

	    if (!fs.exists(execBasePath)) {
	      log.info("The execution base path does not exist. Creating the directory");
	      fs.mkdirs(execBasePath);
	    }
	    if (!fs.exists(execHistory)) {
	      log.info("The history base path does not exist. Creating the directory.");
	      fs.mkdirs(execHistory);
	    }

	    // enforcing max retention on the execution directories to avoid
	    // exceeding HDFS quota. retention is set to a percentage of available
	    // quota.
	    ContentSummary content = fs.getContentSummary(execBasePath);
	    long limit =
	        (long) (content.getQuota() * job.getConfiguration().getFloat(ETL_EXECUTION_HISTORY_MAX_OF_QUOTA, (float) .5));
	    limit = limit == 0 ? 50000 : limit;

	    if (props.containsKey(ETL_BASEDIR_QUOTA_OVERIDE)) {
	      limit = Long.valueOf(props.getProperty(ETL_BASEDIR_QUOTA_OVERIDE));
	    }

	    long currentCount = content.getFileCount() + content.getDirectoryCount();

	    FileStatus[] executions = fs.listStatus(execHistory);
	    Arrays.sort(executions, new Comparator<FileStatus>() {
	      public int compare(FileStatus f1, FileStatus f2) {
	        return f1.getPath().getName().compareTo(f2.getPath().getName());
	      }
	    });

	    // removes oldest directory until we get under required % of count
	    // quota. Won't delete the most recent directory.
	    for (int i = 0; i < executions.length - 1 && limit < currentCount; i++) {
	      FileStatus stat = executions[i];
	      log.info("removing old execution: " + stat.getPath().getName());
	      ContentSummary execContent = fs.getContentSummary(stat.getPath());
	      currentCount -= execContent.getFileCount() + execContent.getDirectoryCount();
	      fs.delete(stat.getPath(), true);
	    }

	    // removing failed exectutions if we need room
	    if (limit < currentCount) {
	      FileStatus[] failedExecutions = fs.listStatus(execBasePath, new PathFilter() {

	        public boolean accept(Path path) {
	          try {
	        	DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss", DateTimeZone.UTC);
	            dateFmt.parseDateTime(path.getName());
	            return true;
	          } catch (IllegalArgumentException e) {
	            return false;
	          }
	        }
	      });

	      Arrays.sort(failedExecutions, new Comparator<FileStatus>() {
	        public int compare(FileStatus f1, FileStatus f2) {
	          return f1.getPath().getName().compareTo(f2.getPath().getName());
	        }
	      });

	      for (int i = 0; i < failedExecutions.length && limit < currentCount; i++) {
	        FileStatus stat = failedExecutions[i];
	        log.info("removing failed execution: " + stat.getPath().getName());
	        ContentSummary execContent = fs.getContentSummary(stat.getPath());
	        currentCount -= execContent.getFileCount() + execContent.getDirectoryCount();
	        fs.delete(stat.getPath(), true);
	      }
	    }

	    // determining most recent execution and using as the starting point for
	    // this execution
	    if (executions.length > 0) {
	      Path previous = executions[executions.length - 1].getPath();
	      FileInputFormat.setInputPaths(job, previous);
	      log.info("Previous execution: " + previous.toString());
	    } else {
	      System.out.println("No previous execution, all topics pulled from earliest available offset");
	    }

	    // creating new execution dir. offsets, error_logs, and count files will
	    // be written to this directory. data is not written to the
	    // output directory in a normal run, but instead written to the
	    // appropriate date-partitioned subdir in camus.destination.path
	    DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss", DateTimeZone.UTC);
	    String executionDate = new DateTime().toString(dateFmt);
	    Path newExecutionOutput = new Path(execBasePath, executionDate);
	    FileOutputFormat.setOutputPath(job, newExecutionOutput);
	    log.info("New execution temp location: " + newExecutionOutput.toString());

	    EtlInputFormat.setLogger(log);
	    job.setMapperClass(EtlMapper.class);

	    job.setInputFormatClass(inputFormatClass);
	    job.setOutputFormatClass(outputFormatClass);
	    job.setNumReduceTasks(0);

	    stopTiming("pre-setup");
	    job.submit();
	    job.waitForCompletion(true);

	    // dump all counters
	    Counters counters = job.getCounters();
	    for (String groupName : counters.getGroupNames()) {
	      CounterGroup group = counters.getGroup(groupName);
	      log.info("Group: " + group.getDisplayName());
	      for (Counter counter : group) {
	        log.info(counter.getDisplayName() + ":\t" + counter.getValue());
	      }
	    }

	    checkIfTooManySkippedMsg(counters);

	    stopTiming("hadoop");
	    startTiming("commit");

	    // Send Tracking counts to Kafka
	    String etlCountsClassName = props.getProperty(ETL_COUNTS_CLASS, ETL_COUNTS_CLASS_DEFAULT);
	    Class<? extends EtlCounts> etlCountsClass = (Class<? extends EtlCounts>) Class.forName(etlCountsClassName);
	    sendTrackingCounts(job, fs, newExecutionOutput, etlCountsClass);

	    Map<EtlKey, ExceptionWritable> errors = readErrors(fs, newExecutionOutput);

	    // Print any potential errors encountered
	    if (!errors.isEmpty())
	      log.error("Errors encountered during job run:");

	    for (Entry<EtlKey, ExceptionWritable> entry : errors.entrySet()) {
	      log.error(entry.getKey().toString());
	      log.error(entry.getValue().toString());
	    }

	    Path newHistory = new Path(execHistory, executionDate);
	    log.info("Moving execution to history : " + newHistory);
	    fs.rename(newExecutionOutput, newHistory);

	    log.info("Job finished");
	    stopTiming("commit");
	    stopTiming("total");
	    createReport(job, timingMap);

	    if (!job.isSuccessful()) {
	      JobClient client = new JobClient(new JobConf(job.getConfiguration()));

	      TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0);

	      for (TaskReport task : client.getMapTaskReports(tasks[0].getTaskAttemptId().getJobID())) {
	        if (task.getCurrentStatus().equals(TIPStatus.FAILED)) {
	          for (String s : task.getDiagnostics()) {
	            System.err.println("task error: " + s);
	          }
	        }
	      }
	      throw new RuntimeException("hadoop job failed");
	    }

	    if (!errors.isEmpty()
	        && props.getProperty(ETL_FAIL_ON_ERRORS, Boolean.FALSE.toString()).equalsIgnoreCase(Boolean.TRUE.toString())) {
	      throw new RuntimeException("Camus saw errors, check stderr");
	    }

	    if (RocketInputFormat.reportJobFailureDueToOffsetOutOfRange) {
	      RocketInputFormat.reportJobFailureDueToOffsetOutOfRange = false;
	      if (props.getProperty(ETL_FAIL_ON_OFFSET_OUTOFRANGE, ETL_FAIL_ON_OFFSET_OUTOFRANGE_DEFAULT)
	        .equalsIgnoreCase(Boolean.TRUE.toString())) {
	        throw new RuntimeException("Some topics skipped due to offsets from Kafka metadata out of range.");
	      }
	    }

	    if (RocketInputFormat.reportJobFailureUnableToGetOffsetFromKafka) {
	      RocketInputFormat.reportJobFailureUnableToGetOffsetFromKafka = false;
	      throw new RuntimeException("Some topics skipped due to failure in getting latest offset from Kafka leaders.");
	    }

	    if (RocketInputFormat.reportJobFailureDueToLeaderNotAvailable) {
	      RocketInputFormat.reportJobFailureDueToLeaderNotAvailable = false;
	      throw new RuntimeException("Some topic partitions skipped due to Kafka leader not available.");
	    }
	  }

	  private void checkIfTooManySkippedMsg(Counters counters) {
	    double maxPercentSkippedSchemaNotFound = Double.parseDouble(props.getProperty(ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND,
	        ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND_DEFAULT));
	    double maxPercentSkippedOther = Double.parseDouble(props.getProperty(ETL_MAX_PERCENT_SKIPPED_OTHER,
	        ETL_MAX_PERCENT_SKIPPED_OTHER_DEFAULT));

	    long actualSkippedSchemaNotFound = 0;
	    long actualSkippedOther = 0;
	    long actualDecodeSuccessful = 0; 
	    for (String groupName : counters.getGroupNames()) {
	      if (groupName.equals(EtlRecordReader.KAFKA_MSG.class.getName())) {
	        CounterGroup group = counters.getGroup(groupName);
	        for (Counter counter : group) {
	          if (counter.getDisplayName().equals(EtlRecordReader.KAFKA_MSG.DECODE_SUCCESSFUL.toString())) {
	            actualDecodeSuccessful = counter.getValue();
	          } else if (counter.getDisplayName().equals(EtlRecordReader.KAFKA_MSG.SKIPPED_SCHEMA_NOT_FOUND.toString())) {
	            actualSkippedSchemaNotFound = counter.getValue();
	          } else if (counter.getDisplayName().equals(EtlRecordReader.KAFKA_MSG.SKIPPED_OTHER.toString())) {
	            actualSkippedOther = counter.getValue();
	          }
	        }
	      }
	    }
	    checkIfTooManySkippedMsg(maxPercentSkippedSchemaNotFound, actualSkippedSchemaNotFound, actualDecodeSuccessful,
	        "schema not found");
	    checkIfTooManySkippedMsg(maxPercentSkippedOther, actualSkippedOther, actualDecodeSuccessful, "other");
	  }

	  private void checkIfTooManySkippedMsg(double maxPercentAllowed, long actualSkipped, long actualSuccessful,
	      String reason) {
	    if (actualSkipped == 0 && actualSuccessful == 0) {
	      return;
	    }
	    double actualSkippedPercent = (double)actualSkipped / (double)(actualSkipped + actualSuccessful) * 100;
	    if (actualSkippedPercent > maxPercentAllowed) {
	      String message =
	          "job failed: " + actualSkippedPercent + "% messages skipped due to " + reason + ", maximum allowed is "
	              + maxPercentAllowed + "%";
	      log.error(message);
	      throw new RuntimeException(message);
	    }
	  }

	  public void cancel() throws IOException {
	    if (this.hadoopJob != null) {
	      this.hadoopJob.killJob();
	    }
	  }

	  public Map<EtlKey, ExceptionWritable> readErrors(FileSystem fs, Path newExecutionOutput) throws IOException {
	    Map<EtlKey, ExceptionWritable> errors = new HashMap<EtlKey, ExceptionWritable>();

	    for (FileStatus f : fs.listStatus(newExecutionOutput, new PrefixFilter(EtlMultiOutputFormat.ERRORS_PREFIX))) {
	      SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), fs.getConf());

	      String errorFrom = "\nError from file [" + f.getPath() + "]";

	      EtlKey key = new EtlKey();
	      ExceptionWritable value = new ExceptionWritable();

	      while (reader.next(key, value)) {
	        ExceptionWritable exceptionWritable = new ExceptionWritable(value.toString() + errorFrom);
	        errors.put(new EtlKey(key), exceptionWritable);
	      }
	      reader.close();
	    }

	    return errors;
	  }

	  // Posts the tracking counts to Kafka
	  public void sendTrackingCounts(JobContext job, FileSystem fs, Path newExecutionOutput,
	      Class<? extends EtlCounts> etlCountsClass) throws IOException, URISyntaxException, IllegalArgumentException,
	      SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException,
	      NoSuchMethodException {
	    if (EtlMultiOutputFormat.isRunTrackingPost(job)) {
	      FileStatus[] gstatuses = fs.listStatus(newExecutionOutput, new PrefixFilter("counts"));
	      HashMap<String, EtlCounts> allCounts = new HashMap<String, EtlCounts>();
	      for (FileStatus gfileStatus : gstatuses) {
	        FSDataInputStream fdsis = fs.open(gfileStatus.getPath());

	        BufferedReader br = new BufferedReader(new InputStreamReader(fdsis), 1048576);
	        StringBuffer buffer = new StringBuffer();
	        String temp = "";
	        while ((temp = br.readLine()) != null) {
	          buffer.append(temp);
	        }
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        ArrayList<EtlCounts> countsObjects =
	            mapper.readValue(buffer.toString(), new TypeReference<ArrayList<EtlCounts>>() {
	            });

	        for (EtlCounts count : countsObjects) {
	          String topic = count.getTopic();
	          if (allCounts.containsKey(topic)) {
	            EtlCounts existingCounts = allCounts.get(topic);
	            existingCounts.setEndTime(Math.max(existingCounts.getEndTime(), count.getEndTime()));
	            existingCounts.setLastTimestamp(Math.max(existingCounts.getLastTimestamp(), count.getLastTimestamp()));
	            existingCounts.setStartTime(Math.min(existingCounts.getStartTime(), count.getStartTime()));
	            existingCounts.setFirstTimestamp(Math.min(existingCounts.getFirstTimestamp(), count.getFirstTimestamp()));
	            existingCounts.setErrorCount(existingCounts.getErrorCount() + count.getErrorCount());
	            existingCounts.setGranularity(count.getGranularity());
	            existingCounts.setTopic(count.getTopic());
	            for (Entry<String, Source> entry : count.getCounts().entrySet()) {
	              Source source = entry.getValue();
	              if (existingCounts.getCounts().containsKey(source.toString())) {
	                Source old = existingCounts.getCounts().get(source.toString());
	                old.setCount(old.getCount() + source.getCount());
	                existingCounts.getCounts().put(old.toString(), old);
	              } else {
	                existingCounts.getCounts().put(source.toString(), source);
	              }
	              allCounts.put(topic, existingCounts);
	            }
	          } else {
	            allCounts.put(topic, count);
	          }
	        }
	      }

	      for (FileStatus countFile : fs.listStatus(newExecutionOutput, new PrefixFilter("counts"))) {
	        if (props.getProperty(ETL_KEEP_COUNT_FILES, "false").equals("true")) {
	          fs.rename(countFile.getPath(), new Path(props.getProperty(ETL_COUNTS_PATH), countFile.getPath().getName()));
	        } else {
	          fs.delete(countFile.getPath(), true);
	        }
	      }

	      String brokerList = getRocketNameServes(job);
	      for (EtlCounts finalEtlCounts : allCounts.values()) {
	        EtlCounts finalCounts = etlCountsClass.getDeclaredConstructor(EtlCounts.class).newInstance(finalEtlCounts);
	        finalCounts.postTrackingCountToKafka(job.getConfiguration(), props.getProperty(KAFKA_MONITOR_TIER),
	            brokerList);
	      }
	    }
	  }

	  /**
	   * Creates a diagnostic report based on provided logger
	   * defaults to TimeLogger which focusses on timing breakdowns. Useful
	   * for determining where to optimize.
	   * 
	   * @param job
	   * @param timingMap
	   * @throws IOException
	   */
	  private void createReport(Job job, Map<String, Long> timingMap) throws IOException, ClassNotFoundException {
	    Class cls = job.getConfiguration().getClassByName(getReporterClass(job));
	    ((BaseReporter) ReflectionUtils.newInstance(cls, job.getConfiguration())).report(job, timingMap);
	  }

	  /**
	   * Path filter that filters based on prefix
	   */
	  private class PrefixFilter implements PathFilter {
	    private final String prefix;

	    public PrefixFilter(String prefix) {
	      this.prefix = prefix;
	    }

	    public boolean accept(Path path) {
	      // TODO Auto-generated method stub
	      return path.getName().startsWith(prefix);
	    }
	  }

	  private String validateFiles(String files, Configuration conf) 
		      throws IOException  {
		    if (files == null) 
		      return null;
		    String[] fileArr = files.split(",");
		    String[] finalArr = new String[fileArr.length];
		    for (int i =0; i < fileArr.length; i++) {
		      String tmp = fileArr[i];
		      String finalPath;
		      URI pathURI;
		      try {
		        pathURI = new URI(tmp);
		      } catch (URISyntaxException e) {
		        throw new IllegalArgumentException(e);
		      }
		      Path path = new Path(pathURI);
		      FileSystem localFs = FileSystem.getLocal(conf);
		      if (pathURI.getScheme() == null) {
		        //default to the local file system
		        //check if the file exists or not first
		        if (!localFs.exists(path)) {
		          throw new FileNotFoundException("File " + tmp + " does not exist.");
		        }
		        finalPath = path.makeQualified(localFs).toString();
		      }
		      else {
		        // check if the file exists in this file system
		        // we need to recreate this filesystem object to copy
		        // these files to the file system jobtracker is running
		        // on.
		        FileSystem fs = path.getFileSystem(conf);
		        if (!fs.exists(path)) {
		          throw new FileNotFoundException("File " + tmp + " does not exist.");
		        }
		        finalPath = path.makeQualified(fs).toString();
		      }
		      finalArr[i] = finalPath;
		    }
		    return StringUtils.arrayToString(finalArr);
	  }
	  /**
	   * If libjars are set in the conf, parse the libjars.
	   * @param conf
	   * @return libjar urls
	   * @throws IOException
	   */
	  public static URL[] getLibJars(Configuration conf) throws IOException {
	    String jars = conf.get("tmpjars");
	    if(jars==null) {
	      return null;
	    }
	    String[] files = jars.split(",");
	    List<URL> cp = new ArrayList<URL>();
	    for (String file : files) {
	      Path tmp = new Path(file);
	      if (tmp.getFileSystem(conf).equals(FileSystem.getLocal(conf))) {
	        cp.add(FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL());
	      } else {
	        log.warn("The libjars file " + tmp + " is not on the local " +
	          "filesystem. Ignoring.");
	      }
	    }
	    return cp.toArray(new URL[0]);
	  }
	  public static void main(String[] args) throws Exception {
		  /*String[] values = {
			"-P"
			,"E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/camus.properties"
			,"-libjars"
			,"E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/camus-api-0.1.0-SNAPSHOT.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/camus-example-0.1.0-SNAPSHOT.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/camus-kafka-coders-0.1.0-SNAPSHOT.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/camus-schema-registry-0.1.0-SNAPSHOT.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/joda-time-1.6.2.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/kafka_2.10-0.8.0.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/metrics-core-2.2.0.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/netty-all-4.0.25.Final.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/rocketmq-client-3.2.6.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/rocketmq-common-3.2.6.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/rocketmq-namesrv-3.2.6.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/rocketmq-remoting-3.2.6.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/rocketmq-tools-3.2.6.jar,E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/jar/lib/scala-library-2.10.3.jar"
		  };*/
		  /*
		Configuration conf = new Configuration();  
	    String[] otherArgs = new GenericOptionsParser(conf, values).getRemainingArgs();
	    */
//		job.run(values);
;
//		 String msg = job.validateFiles("/home/guodongdong/rocketmq-camus/lib/camus-api-0.1.0-SNAPSHOT.jar,/home/guodongdong/rocketmq-camus/lib/camus-example-0.1.0-SNAPSHOT.jar,/home/guodongdong/rocketmq-camus/lib/camus-kafka-coders-0.1.0-SNAPSHOT.jar,/home/guodongdong/rocketmq-camus/lib/camus-schema-registry-0.1.0-SNAPSHOT.jar,/home/guodongdong/rocketmq-camus/lib/joda-time-1.6.2.jar,/home/guodongdong/rocketmq-camus/lib/kafka_2.10-0.8.0.jar,/home/guodongdong/rocketmq-camus/lib/metrics-core-2.2.0.jar,/home/guodongdong/rocketmq-camus/lib/netty-all-4.0.25.Final.jar,/home/guodongdong/rocketmq-camus/lib/rocketmq-client-3.2.6.jar,/home/guodongdong/rocketmq-camus/lib/rocketmq-common-3.2.6.jar,/home/guodongdong/rocketmq-camus/lib/rocketmq-namesrv-3.2.6.jar,/home/guodongdong/rocketmq-camus/lib/rocketmq-remoting-3.2.6.jar,/home/guodongdong/rocketmq-camus/lib/rocketmq-tools-3.2.6.jar,/home/guodongdong/rocketmq-camus/lib/scala-library-2.10.3.jar", job.getConf());
//		 System.out.println(msg);
			RocketJob job = new RocketJob();
			ToolRunner.run(job, args);
	  }

	  @SuppressWarnings("static-access")
	  @Override
	  public int run(String[] args) throws Exception {
	    Options options = new Options();

	    options.addOption("p", true, "properties filename from the classpath");
	    options.addOption("P", true, "external properties filename (hdfs: or local FS)");
	    options.addOption("libjars", true, "external libjars");

	    options.addOption(OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator()
	        .withDescription("use value for given property").create("D"));

	    CommandLineParser parser = new PosixParser();
	    CommandLine cmd = parser.parse(options, args,true);

	    if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
	      HelpFormatter formatter = new HelpFormatter();
	      formatter.printHelp("CamusJob.java", options);
	      return 1;
	    }
	   

	    if (cmd.hasOption('p'))
	      props.load(this.getClass().getClassLoader().getResourceAsStream(cmd.getOptionValue('p')));

	    
	    if (cmd.hasOption("libjars")) {
	        props.setProperty("tmpjars",cmd.getOptionValue("libjars"));
	    }
	    log.info("libjars path:"+cmd.getOptionValue("libjars"));
	    System.out.println("libjars path:"+cmd.getOptionValue("libjars"));
	    Option[] optis = cmd.getOptions();
	    for (Option option : optis) {
	    	log.info("option:"+option.getValue());
			System.out.println("option:"+option.getValue());
		}
	    if (cmd.hasOption("libjars")) {
	    	System.out.println("1728 gdd libjars path:"+cmd.getOptionValue("libjars"));
	        this.getConf().set("tmpjars", 
	                 this.validateFiles(cmd.getOptionValue("libjars"), this.getConf()),
	                 "from -libjars command line option");
	        //setting libjars in client classpath
	        URL[] libjars = getLibJars(this.getConf());
	        if(libjars!=null && libjars.length>0) {
	          this.getConf().setClassLoader(new URLClassLoader(libjars, this.getConf().getClassLoader()));
	          Thread.currentThread().setContextClassLoader(
	              new URLClassLoader(libjars, 
	                  Thread.currentThread().getContextClassLoader()));
	        }
	    }
	    if (cmd.hasOption('P')) {
	      String pathname = cmd.getOptionValue('P');

	      InputStream fStream;
	      if (pathname.startsWith("hdfs:")) {
	        Path pt = new Path(pathname);
	        FileSystem fs = FileSystem.get(new Configuration());
	        fStream = fs.open(pt);
	      } else {
	        File file = new File(pathname);
	        fStream = new FileInputStream(file);
	      }

	      props.load(fStream);
	      fStream.close();
	    }
	    

	    props.putAll(cmd.getOptionProperties("D"));
	    
	    run();
	    return 0;
	  }

	  // Temporarily adding all Kafka parameters here
	  public static boolean getPostTrackingCountsToKafka(Job job) {
	    return job.getConfiguration().getBoolean(POST_TRACKING_COUNTS_TO_KAFKA, true);
	  }

	  public static int getKafkaFetchRequestMinBytes(JobContext context) {
	    return context.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MIN_BYTES, 1024);
	  }

	  public static int getKafkaFetchRequestMaxWait(JobContext job) {
	    return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MAX_WAIT, 1000);
	  }

	  public static String getRocketNameServes(JobContext job) {
	    String nameadddrs = job.getConfiguration().get(ROCKET_NAMESRVADDR);
	    if (nameadddrs == null) {
	        log.warn("The configuration properties " + ROCKET_HOST_URL + " and " + ROCKET_HOST_PORT
	            + " are deprecated. Please switch to using " + ROCKET_NAMESRVADDR);
	        return nameadddrs + ":" + job.getConfiguration().getInt(ROCKET_HOST_PORT, 10251);
	    }
	    return nameadddrs;
	  }

	  public static int getKafkaFetchRequestCorrelationId(JobContext job) {
	    return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_CORRELATION_ID, -1);
	  }

	  public static String getRocketClientName(JobContext job) {
	    return job.getConfiguration().get(ROCKET_CLIENT_NAME);
	  }
	  
	  public static String getRocketPullTimeOutMillts(JobContext job) {
		    return job.getConfiguration().get(ROCKET_PULL_TIME_OUT_MILLIS);
	  }
	  
	  public static String getRocketBrokerSuspendMaxTimeMillts(JobContext job) {
		    return job.getConfiguration().get(ROCKET_BROKER_SUSPEND_MAX_TIME_MILLIS);
	  }
	  
	  public static String getKafkaFetchRequestBufferSize(JobContext job) {
	    return job.getConfiguration().get(ROCKET_FETCH_BUFFER_SIZE);
	  }

	  public static int getKafkaTimeoutValue(JobContext job) {
	    int timeOut = job.getConfiguration().getInt(KAFKA_TIMEOUT_VALUE, 30000);
	    return timeOut;
	  }

	  public static int getKafkaBufferSize(JobContext job) {
	    return job.getConfiguration().getInt(ROCKET_FETCH_BUFFER_SIZE, 1024 * 1024);
	  }

	  public static boolean getLog4jConfigure(JobContext job) {
	    return job.getConfiguration().getBoolean(LOG4J_CONFIGURATION, false);
	  }

	  public static String getReporterClass(JobContext job) {
	    return job.getConfiguration().get(CAMUS_REPORTER_CLASS, "com.linkedin.camus.etl.kafka.reporter.TimeReporter");
	  }
	}