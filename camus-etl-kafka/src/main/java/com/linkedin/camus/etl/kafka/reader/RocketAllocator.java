package com.linkedin.camus.etl.kafka.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.workallocater.CamusRequest;
import com.linkedin.camus.workallocater.WorkAllocator;

public class RocketAllocator extends WorkAllocator {

	private static Logger log = Logger.getLogger(RocketReader.class);
	
	  protected Properties props;

	  public void init(Properties props) {
	    this.props = props;
	  }

	  protected void reverseSortRequests(List<CamusRequest> requests) {
	    // Reverse sort by size
	    Collections.sort(requests, new Comparator<CamusRequest>() {
	      @Override
	      public int compare(CamusRequest o1, CamusRequest o2) {
	        if (o2.estimateDataSize() == o1.estimateDataSize()) {
	          return 0;
	        }
	        if (o2.estimateDataSize() < o1.estimateDataSize()) {
	          return -1;
	        } else {
	          return 1;
	        }
	      }
	    });
	  }

	  @Override
	  public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context) throws IOException {
		log.info("[RocketAllocator.allocateWork][begin]");
	    int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);

	    reverseSortRequests(requests);

	    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

	    for (int i = 0; i < numTasks; i++) {
	      if (requests.size() > 0) {
	        kafkaETLSplits.add(new EtlRocketSplit());
	      }
	    }

	    for (CamusRequest r : requests) {
	    	log.info("rocketallocator request:"+r.toString());
	    	getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
	    }
	    for (InputSplit inputSplit : kafkaETLSplits) {
	    	log.info("[RocketAllocator.kafkaETLSplits]:"+inputSplit.toString());
		}
	    
	    log.info("[RocketAllocator.allocateWork][end]");
	    return kafkaETLSplits;
	  }

	  protected EtlRocketSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits) throws IOException {
		  log.info("[RocketAllocator.getSmallestMultiSplit][begin]");
		  EtlRocketSplit smallest = (EtlRocketSplit) kafkaETLSplits.get(0);

	    for (int i = 1; i < kafkaETLSplits.size(); i++) {
	    	EtlRocketSplit challenger = (EtlRocketSplit) kafkaETLSplits.get(i);
	      if ((smallest.getLength() == challenger.getLength() && smallest.getNumRequests() > challenger.getNumRequests())
	          || smallest.getLength() > challenger.getLength()) {
	        smallest = challenger;
	      }
	    }
	    log.info("[RocketAllocator.getSmallestMultiSplit][end]");
	    return smallest;
	  }

	}