package com.linkedin.camus.etl.kafka.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import com.linkedin.camus.workallocater.CamusRequest;
import com.linkedin.camus.workallocater.WorkAllocator;

public class RocketInputFormat extends InputFormat<EtlKey, CamusWrapper> {

    public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
    public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

    public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";
    public static final String KAFKA_MOVE_TO_EARLIEST_OFFSET = "kafka.move.to.earliest.offset";

    public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
    public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

  public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
  public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
  public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

  public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
  public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
  public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

  public static final String CAMUS_WORK_ALLOCATOR_CLASS = "camus.work.allocator.class";
  public static final String CAMUS_WORK_ALLOCATOR_DEFAULT = "com.linkedin.camus.workallocater.BaseAllocator";

  public static final int NUM_TRIES_PARTITION_METADATA = 3;
  public static final int NUM_TRIES_FETCH_FROM_LEADER = 3;
  public static final int NUM_TRIES_TOPIC_METADATA = 3;

  public static boolean reportJobFailureDueToOffsetOutOfRange = false;
  public static boolean reportJobFailureUnableToGetOffsetFromKafka = false;
  public static boolean reportJobFailureDueToLeaderNotAvailable = false;

   private static Logger log = null;

	 public RocketInputFormat(){
		 if(log==null)
			 log = Logger.getLogger(getClass());
	 }
	@Override
	public RecordReader<EtlKey, CamusWrapper> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new EtlRocketRecordReader(this, split, context);
	}

	
	
	
	public Set<String> filterWhitelistTopics(Set<String> topicLists,
		      Set<String> whiteListTopics) {
		    Set<String> filteredTopics = new HashSet<String>();
		    String regex = createTopicRegEx(whiteListTopics);
		    for (String topic : topicLists) {
		      if (Pattern.matches(regex, topic)) {
		        filteredTopics.add(topic);
		      } else {
		        log.info("Discarding topic : " + topic);
		      }
		    }
		    return filteredTopics;
	}
	public String createTopicRegEx(Set<String> topicsSet) {
	    String regex = "";
	    StringBuilder stringbuilder = new StringBuilder();
	    for (String whiteList : topicsSet) {
	      stringbuilder.append(whiteList);
	      stringbuilder.append("|");
	    }
	    regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1) + ")";
	    Pattern.compile(regex);
	    return regex;
	  }
	
	public static String[] getKafkaWhitelistTopic(JobContext job) {
	    return getKafkaWhitelistTopic(job.getConfiguration());
	  }

	  public static String[] getKafkaWhitelistTopic(Configuration conf) {
	    final String whitelistStr = conf.get(KAFKA_WHITELIST_TOPIC);
	    if (whitelistStr != null && !whitelistStr.isEmpty()) {
	      return conf.getStrings(KAFKA_WHITELIST_TOPIC);
	    } else {
	      return new String[] {};
	    }
	  }
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		RocketJob.startTiming("getSplits");
	    ArrayList<CamusRequest> finalRequests = new ArrayList<CamusRequest>();
		try{
			DefaultMQAdminExt ext = new DefaultMQAdminExt();
	    	ext.setNamesrvAddr(RocketJob.getRocketNameServes(context));
	    	ext.start();
			TopicList topLists = ext.fetchAllTopicList();
			Set<String> topics = topLists.getTopicList();
			ext.shutdown();
			HashSet<String> whiteListTopics = new HashSet<String>(Arrays.asList(getKafkaWhitelistTopic(context)));
			if(!whiteListTopics.isEmpty()){
				topics = filterWhitelistTopics(topics, whiteListTopics);
			}
		      // Filter all blacklist topics
		      HashSet<String> blackListTopics = new HashSet<String>(Arrays.asList(getKafkaBlacklistTopic(context)));
		      String regex = "";
		      if (!blackListTopics.isEmpty()) {
		        regex = createTopicRegEx(blackListTopics);
		      }
		    	DefaultMQPullConsumer consumer = createDefaultMQPullConsumer(context);
	        	consumer.start();
			  for (String topic : topics) {
			        if (Pattern.matches(regex, topic)) {
			          log.info("Discarding topic (blacklisted): " + topic);
			        } else if (!createMessageDecoder(context, topic)) {
			          log.info("Discarding topic (Decoder generation failed) : " + topic);
			        } else{
			        	log.info("topic:"+topic);
			        	Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
			        	log.info("mqsSize:"+mqs.size());
			        	if(mqs!=null&&mqs.size()>0){
			        		for (MessageQueue msgQue : mqs) {
			        			PullResult pullResult = consumer.pullBlockIfNotFound(msgQue, null, 0, 3);
			        			RocketRequest rocketRequest = new RocketRequest(context,topic,msgQue.getQueueId());
			        			rocketRequest.setLatestOffset(pullResult.getMaxOffset());
			        			rocketRequest.setEarliestOffset(0);
			        			finalRequests.add(rocketRequest);
							}
			        	}
			        }
				}
	          consumer.shutdown();
			  Collections.sort(finalRequests, new Comparator<CamusRequest>() {
			      @Override
			      public int compare(CamusRequest r1, CamusRequest r2) {
			        return r1.getTopic().compareTo(r2.getTopic());
			      }
			    });
			  log.info("The requests from kafka metadata are: \n" + finalRequests);
			  wrieteRequests(finalRequests, context);
			  Map<CamusRequest, EtlKey> offsetKeys = getPreviousOffsets(FileInputFormat.getInputPaths(context), context);
			  Set<String> moveLatest = getMoveToLatestTopicsSet(context);
			  for (CamusRequest request : finalRequests) {

			      if (moveLatest.contains(request.getTopic()) || moveLatest.contains("all")) {
			        log.info("Moving to latest for topic: " + request.getTopic());
			        //TODO: factor out kafka specific request functionality
			        EtlKey oldKey = offsetKeys.get(request);
			        EtlKey newKey =
			            new EtlKey(request.getTopic(), ((RocketRequest) request).getLeaderId(), request.getPartition(), 0,
			                request.getLastOffset());

			        if (oldKey != null)
			          newKey.setMessageSize(oldKey.getMessageSize());

			        offsetKeys.put(request, newKey);
			      }

			      EtlKey key = offsetKeys.get(request);

			      if (key != null) {
			        request.setOffset(key.getOffset());
			        request.setAvgMsgSize(key.getMessageSize());
			      }

			      if (request.getEarliestOffset() > request.getOffset() || request.getOffset() > request.getLastOffset()) {
			        if (request.getEarliestOffset() > request.getOffset()) {
			          log.error("The earliest offset was found to be more than the current offset: " + request);
			        } else {
			          log.error("The current offset was found to be more than the latest offset: " + request);
			        }

			        boolean move_to_earliest_offset = context.getConfiguration().getBoolean(KAFKA_MOVE_TO_EARLIEST_OFFSET, false);
			        boolean offsetUnset = request.getOffset() == EtlRequest.DEFAULT_OFFSET;
			        log.info("move_to_earliest: " + move_to_earliest_offset + " offset_unset: " + offsetUnset);
			        // When the offset is unset, it means it's a new topic/partition, we also need to consume the earliest offset
			        if (move_to_earliest_offset || offsetUnset) {
			          log.error("Moving to the earliest offset available");
			          request.setOffset(request.getEarliestOffset());
			          offsetKeys.put(
			              request,
			              //TODO: factor out kafka specific request functionality
			              new EtlKey(request.getTopic(), ((EtlRequest) request).getLeaderId(), request.getPartition(), 0, request
			                  .getOffset()));
			        } else {
			          log.error("Offset range from kafka metadata is outside the previously persisted offset, " + request + "\n" +
			                    " Topic " + request.getTopic() + " will be skipped.\n" +
			                    " Please check whether kafka cluster configuration is correct." +
			                    " You can also specify config parameter: " + KAFKA_MOVE_TO_EARLIEST_OFFSET +
			                    " to start processing from earliest kafka metadata offset.");
			          reportJobFailureDueToOffsetOutOfRange = true;
			        }
			      }
			      log.info(request);
			  }
		   writePrevious(offsetKeys.values(), context);
		}catch(Exception e){
			  log.error("Unable to pull requests from Kafka brokers. Exiting the program", e);
		      throw new IOException("Unable to pull requests from Kafka brokers.", e);
		}

	    CamusJob.stopTiming("getSplits");
	    CamusJob.startTiming("hadoop");
	    CamusJob.setTime("hadoop_start");

	    WorkAllocator allocator = getWorkAllocator(context);
	    Properties props = new Properties();
	    props.putAll(context.getConfiguration().getValByRegex(".*"));
	    allocator.init(props);
		return allocator.allocateWork(finalRequests, context);
	}
	
	  public static WorkAllocator getWorkAllocator(JobContext job) {
	    try {
	      return (WorkAllocator) job.getConfiguration()
	          .getClass(CAMUS_WORK_ALLOCATOR_CLASS, Class.forName(CAMUS_WORK_ALLOCATOR_DEFAULT)).newInstance();
	    } catch (Exception e) {
	      throw new RuntimeException(e);
	    }
	  }
	
	  private void writePrevious(Collection<EtlKey> missedKeys, JobContext context) throws IOException {
		    FileSystem fs = FileSystem.get(context.getConfiguration());
		    Path output = FileOutputFormat.getOutputPath(context);

		    if (!fs.exists(output)) {
		      fs.mkdirs(output);
		    }

		    output = new Path(output, EtlMultiOutputFormat.OFFSET_PREFIX + "-previous");
		    SequenceFile.Writer writer =
		        SequenceFile.createWriter(fs, context.getConfiguration(), output, EtlKey.class, NullWritable.class);

		    for (EtlKey key : missedKeys) {
		      writer.append(key, NullWritable.get());
		    }

		    writer.close();
		  }
	
	  private Set<String> getMoveToLatestTopicsSet(JobContext context) {
		    Set<String> topics = new HashSet<String>();

		    String[] arr = getMoveToLatestTopics(context);

		    if (arr != null) {
		      for (String topic : arr) {
		        topics.add(topic);
		      }
		    }

		    return topics;
	  }
	  public static String[] getMoveToLatestTopics(JobContext job) {
		    return job.getConfiguration().getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
		  }
	  private Map<CamusRequest, EtlKey> getPreviousOffsets(Path[] inputs, JobContext context) throws IOException {
		    Map<CamusRequest, EtlKey> offsetKeysMap = new HashMap<CamusRequest, EtlKey>();
		    for (Path input : inputs) {
		      FileSystem fs = input.getFileSystem(context.getConfiguration());
		      for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
		        log.info("previous offset file:" + f.getPath().toString());
		        SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), context.getConfiguration());
		        EtlKey key = new EtlKey();
		        while (reader.next(key, NullWritable.get())) {
		          //TODO: factor out kafka specific request functionality
		          CamusRequest request = new RocketRequest(context, key.getTopic(),key.getPartition());
		          if (offsetKeysMap.containsKey(request)) {

		            EtlKey oldKey = offsetKeysMap.get(request);
		            if (oldKey.getOffset() < key.getOffset()) {
		              offsetKeysMap.put(request, key);
		            }
		          } else {
		        	  offsetKeysMap.put(request, key);
		          }
		          key = new EtlKey();
		        }
		        reader.close();
		      }
		    }
		    return offsetKeysMap;
		  }
	  
	protected void wrieteRequests(List<CamusRequest> requests,JobContext context) throws IOException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
	    Path output = FileOutputFormat.getOutputPath(context);
	    if(!fs.exists(output)){
	    	fs.mkdirs(output);
	    }
	    output = new Path(output,EtlMultiOutputFormat.REQUESTS_FILE);
	    SequenceFile.Writer writer =
	            SequenceFile.createWriter(fs, context.getConfiguration(), output, RocketRequest.class, NullWritable.class);
	    
	    for (CamusRequest r : requests) {
	        //TODO: factor out kafka specific request functionality
	        writer.append(r, NullWritable.get());
	      }
	      writer.close();

	}
	public ArrayList<RocketRequest> fetchMessageQueusAndCreateRequests(JobContext context,HashMap<String,Set<MessageQueue>> messageQueues){
		ArrayList<RocketRequest> finalRockets = new ArrayList<RocketRequest>();
		for (String topic : messageQueues.keySet()) {
			Set<MessageQueue> msgQueues = messageQueues.get(topic);
			for (MessageQueue messageQueue : msgQueues) {
//				messageQueue
			}
		}
		
		return null;
	}
	
	
	public DefaultMQPullConsumer createDefaultMQPullConsumer(JobContext context) {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(RocketJob.getRocketClientName(context));
		consumer.setNamesrvAddr(RocketJob.getRocketNameServes(context));
		consumer.setConsumerPullTimeoutMillis(Long.valueOf(RocketJob.getRocketPullTimeOutMillts(context)));
		consumer.setBrokerSuspendMaxTimeMillis(Long.valueOf(RocketJob.getRocketBrokerSuspendMaxTimeMillts(context)));
	    return consumer;
    }
	
	private boolean createMessageDecoder(JobContext context, String topic) {
	    try {
	      MessageDecoderFactory.createMessageDecoder(context, topic);
	      return true;
	    } catch (Exception e) {
	      log.error("failed to create decoder", e);
	      return false;
	    }
	  }
	  public static String[] getKafkaBlacklistTopic(JobContext job) {
		    return getKafkaBlacklistTopic(job.getConfiguration());
		 }

	  public static String[] getKafkaBlacklistTopic(Configuration conf) {
		    final String blacklistStr = conf.get(KAFKA_BLACKLIST_TOPIC);
		    if (blacklistStr != null && !blacklistStr.isEmpty()) {
		      return conf.getStrings(KAFKA_BLACKLIST_TOPIC);
		    } else {
		      return new String[] {};
		    }
	  }
	  private class OffsetFileFilter implements PathFilter {

		    @Override
		    public boolean accept(Path arg0) {
		      return arg0.getName().startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
		    }
		  }
}
