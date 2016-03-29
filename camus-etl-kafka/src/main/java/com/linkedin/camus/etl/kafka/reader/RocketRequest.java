package com.linkedin.camus.etl.kafka.reader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.camus.etl.kafka.common.EtlRequest;

public class RocketRequest extends EtlRequest {
    private String brokerName;
    private int queueId;
    private AtomicLong currentOffset = new AtomicLong(0);
    
    /**
     * Constructor for the KafkaETLRequest with the offset to -1.
     * 
     * @param topic
     *            The topic name
     * @param leaderId
     *            The leader broker for this topic and partition
     * @param partition
     *            The partition to pull
     * @param brokerUri
     *            The uri for the broker.
     */
    public RocketRequest(){
    	
    }
    public RocketRequest(JobContext context, String topic,int queId) {
      super(context, topic,topic.hashCode()+"",queId,null, DEFAULT_OFFSET);
      this.queueId = queId;
    }
    
	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public AtomicLong getCurrentOffset() {
		return currentOffset;
	}

	public void setCurrentOffset(AtomicLong currentOffset) {
		this.currentOffset = currentOffset;
	}
	
	@Override
	  public String toString() {
	    return this.getTopic() + "\turi:" + (this.getURI() != null ? this.getURI().toString() : "") + "\tleader:" + this.getLeaderId() + "\tpartition:" + this.getPartition()
	        + "\tearliest_offset:" + getEarliestOffset() + "\toffset:" + this.getOffset()+ "\tlatest_offset:" + getLastOffset()
	        + "\testimated_size:" + estimateDataSize()+ "\tqueId:" + this.getQueueId()+ "\tcurrentoffset:" + this.getCurrentOffset();
	  }
	
	 @Override
	  public void readFields(DataInput in) throws IOException {
	    this.setTopic(UTF8.readString(in));
	    this.setLeaderId(UTF8.readString(in));
	    String str = UTF8.readString(in);
	    if (!str.isEmpty())
	      try {
	    	  this.setURI(new URI(str));
	      } catch (URISyntaxException e) {
	        throw new RuntimeException(e);
	      }
	    this.setPartition(in.readInt());
	    this.setOffset(in.readLong());
	    this.setLatestOffset(in.readLong());
	    this.setQueueId(in.readInt());
	 }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    UTF8.writeString(out, this.getTopic()==null?"":this.getTopic());
	    UTF8.writeString(out, this.getLeaderId()==null?"":this.getLeaderId());
	    if (this.getURI() != null)
	      UTF8.writeString(out, this.getURI().toString());
	    else
	      UTF8.writeString(out, "");
	    out.writeInt(this.getPartition());
	    out.writeLong(this.getOffset());
	    out.writeLong(this.getLastOffset());
	    out.writeInt(this.getQueueId());
	    
	  }
	
	 
}
