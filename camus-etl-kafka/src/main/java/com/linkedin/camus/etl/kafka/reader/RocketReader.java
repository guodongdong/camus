package com.linkedin.camus.etl.kafka.reader;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.linkedin.camus.etl.kafka.common.EtlKey;

public class RocketReader {

	private static Logger log = Logger.getLogger(RocketReader.class);
	private RocketRequest rocketRequest = null;
	private DefaultMQPullConsumer pullConsumer = null;
	
	private long beginOffset;
	private long lastOffset;
	
	private TaskAttemptContext context;
	private Iterator<MessageExt> messageIter = null;
	
	private long totalFetchTime = 0;
	private long lastFetchTime = 0;
	
	private int fetchSize;
	
	
	public RocketReader(RocketInputFormat inputFormat,TaskAttemptContext context,RocketRequest request,
			int clientTimeOut,int fetchSize) throws MQClientException{
		this.fetchSize = fetchSize;
		this.context = context;
		log.info("bufferSize="+fetchSize);
		log.info("timeOut="+clientTimeOut);
		rocketRequest = request;
		beginOffset = request.getOffset();
		//currentsize
		request.setCurrentOffset(new AtomicLong(request.getOffset()));
		lastOffset = request.getLastOffset();
		pullConsumer = this.createDefaultMQPullConsumer(context);
		pullConsumer.start();
		log.info("Connected to rocketMq  begint reading at offset");
		fetch();
	}
	
	public DefaultMQPullConsumer createDefaultMQPullConsumer(JobContext context) {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(RocketJob.getRocketClientName(context));
		consumer.setNamesrvAddr(RocketJob.getRocketNameServes(context));
		consumer.setConsumerPullTimeoutMillis(Long.valueOf(RocketJob.getRocketPullTimeOutMillts(context)));
		consumer.setBrokerSuspendMaxTimeMillis(Long.valueOf(RocketJob.getRocketBrokerSuspendMaxTimeMillts(context)));
	    return consumer;
    }
	public boolean hadNext(){
		log.info("[RocketReader.hadNext][begin]");
		if(messageIter!=null&&messageIter.hasNext())
			return true;
		else 
			return fetch();
	}
	public boolean getNext(EtlKey key,BytesWritable payload,BytesWritable pkey){
		log.info("[RocketReader.getNext][begin]");
		if(hadNext()){
			MessageExt msgExt = messageIter.next();
			log.info("msg ext:"+new String(msgExt.getBody()));
			byte[] bytes = msgExt.getBody();
			payload.set(bytes, 0, bytes.length);
			String msgId = msgExt.getMsgId();
			if(msgId!=null){
				byte[] msgIdBytes = msgId.getBytes();
				pkey.set(msgIdBytes, 0, msgIdBytes.length);
			}else{
				  log.warn("Received message with null message.key(): " + msgExt.toString());
				pkey.setSize(0);
			}
			key.clear();
		    key.set(rocketRequest.getTopic(), rocketRequest.getLeaderId(), rocketRequest.getPartition(),rocketRequest.getCurrentOffset().get(),
		    		msgExt.getQueueOffset()+1,Long.valueOf(msgExt.getProperty("MAX_OFFSET")));
			key.setMessageSize(Long.valueOf(msgExt.getProperty("MAX_OFFSET")));
			rocketRequest.getCurrentOffset().incrementAndGet();
			log.info("[RocketReader.getNext][end]");
			return true;
		}else{
			log.info("[RocketReader.getNext][end]");
			return false;
		}
	}
	public boolean fetch(){
		log.info("[RocketReader.fetch][begin]");
		if(rocketRequest.getCurrentOffset().get()>=lastOffset){
			return false;
		}
		long tempTime = System.currentTimeMillis();
		log.info("\nAsking for offset : " + (rocketRequest.getCurrentOffset().get()));
		
		try{
			Set<MessageQueue> msgQues = pullConsumer.fetchSubscribeMessageQueues(rocketRequest.getTopic());
			for (MessageQueue msgQue : msgQues) {
				if(msgQue.getQueueId()==rocketRequest.getQueueId()){
					log.info("rocketrequest:"+rocketRequest.toString());
					return processFetchResponse(msgQue,tempTime);
				}
			}
		}catch (Exception e) {
			  log.info("error msg:"+e.getMessage());
			  log.info("error content:"+e);
			  log.info("Exception generated during fetch for topic " + rocketRequest.getTopic()
		              + ": " + e.getMessage() + ". Will refresh topic metadata and retry.");
//		          return refreshTopicMetadataAndRetryFetch(rocketRequest, tempTime);
		      return false;
		}
		log.info("[RocketReader.fetch][end]");
		return false;
	}
	public void close(){
		if(pullConsumer!=null){
			pullConsumer.shutdown();
		}
	}
	/**
	   * Returns the total bytes that will be fetched. This is calculated by
	   * taking the diffs of the offsets
	   *
	   * @return
	   */
	  public long getTotalBytes() {
	    return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
	  }

	  /**
	   * Returns the total bytes that have been fetched so far
	   *
	   * @return
	   */
	  public long getReadBytes() {
	    return rocketRequest.getCurrentOffset().get() - beginOffset;
	  }

	  /**
	   * Returns the number of events that have been read r
	   *
	   * @return
	   */
	  public long getCount() {
	    return rocketRequest.getCurrentOffset().longValue();
	  }

	  /**
	   * Returns the fetch time of the last fetch in ms
	   *
	   * @return
	   */
	  public long getFetchTime() {
	    return lastFetchTime;
	  }

	  /**
	   * Returns the totalFetchTime in ms
	   *
	   * @return
	   */
	  public long getTotalFetchTime() {
	    return totalFetchTime;
	  }
	public boolean processFetchResponse(MessageQueue mq,long tempTime){
		log.info("[RocketReader.processFetchResponse][begin]");
		try{
				lastFetchTime = (System.currentTimeMillis()-tempTime);
				log.info("mq:"+mq);
				PullResult pullResult = pullConsumer.pullBlockIfNotFound(mq, null, rocketRequest.getCurrentOffset().get(), fetchSize);
				int skipped = 0;
				totalFetchTime += lastFetchTime;
				log.info("[pullResult.getPullStatus][status]"+pullResult.getPullStatus());
                switch (pullResult.getPullStatus()) {
                case FOUND:
					 messageIter = pullResult.getMsgFoundList().iterator();
                	if(pullResult.getNextBeginOffset()<rocketRequest.getCurrentOffset().get()){
                		skipped++;
                	}else{
                        log.debug("Skipped offsets till : " + pullResult.getNextBeginOffset());
                        break;
                	}
                	
                	log.debug("Number of offsets to be skipped: " + skipped);
                    while (skipped != 0) {
                    	List<MessageExt> msgExts=pullResult.getMsgFoundList();
						if(msgExts!=null&&msgExts.size()>0){
							for (MessageExt messageExt : msgExts) {
								log.debug("Skipping offset : " + messageExt.hashCode());
							}
						}
                      skipped--;
                    }
                    if(!messageIter.hasNext()){
                    	 System.out.println("No more data left to process. Returning false");
                         messageIter = null;
                         return false;
                    }
                    break;
                case NO_MATCHED_MSG:
                    break;
                case NO_NEW_MSG:
                    break;
                case OFFSET_ILLEGAL:
                    break;
                default:
                    break;
	       }
		      log.info("[RocketReader.processFetchResponse][end]");
		  	return true;
		 }catch (Exception e) {
		      log.info("Exception generated during processing fetchResponse");
			  log.info("[RocketReader.processFetchResponse][end]");
		      return false;
		 }
	}
}
