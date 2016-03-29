package com.linkedin.camus.etl.kafka.gdd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.workallocater.CamusRequest;

public class PriviouseOffsetReader {

	private static Logger log = Logger.getLogger(PriviouseOffsetReader.class);
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			FSDataInputStream in = null;
			FileSystem fs = FileSystem.get(conf);
			///work/guodongdong/rocketmq/history/2015-05-12-03-43-42/offsets-previous
			//hdfs://10.0.50.10:8020/
			//hdfs://10.0.50.10:8020/work/guodongdong/rocketmq/history/2015-05-12-03-47-38/offsets-previous
			///work/guodongdong/rocketmq/history/2015-05-12-11-38-48/requests.previous
			Map<CamusRequest, EtlKey> offsetKeysMap = new HashMap<CamusRequest, EtlKey>();
			//F:\dddd\20150504\10\history
			//F:/dddd/20150504/10/2015-05-10-16-05-35
			//"F:/dddd/20150504/10/history/2015-05-13-11-00-46
			//F:/dddd/20150504/10/11/2015-05-10-16-05-35
			for (FileStatus f : fs.listStatus(new Path("E:/kuaidi/warehouse/trunk/realtime/2015-05-25-16-05-05"),new PathFilter() {
				@Override
				public boolean accept(Path path) {
					return path.getName().startsWith("offsets");
				}
			})) {
//				System.out.println(f.getPath().toString());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs,f.getPath(), conf);
				EtlKey key = new EtlKey();
//			    ExceptionWritable value = new ExceptionWritable();
//			    NullWritable value = new NullWritable();
				EtlRequest eltRequest = new EtlRequest();
//			   String errorFrom = "\nError from file [" + f.getPath() + "]";
				while (reader.next(key,NullWritable.get())) {
//					System.out.println(key.toString());
				  //TODO: factor out kafka specific request functionality
//				 ExceptionWritable exceptionWritable = new ExceptionWritable(value.toString() + errorFrom);
//				 System.out.println(eltRequest.toString());
					
				 CamusRequest request = new EtlRequest(null, key.getTopic(), key.getLeaderId(), key.getPartition());
				 if (offsetKeysMap.containsKey(request)) {

				    EtlKey oldKey = offsetKeysMap.get(request);
				    if (oldKey.getOffset() < key.getOffset()) {
				      offsetKeysMap.put(request, key);
				    }
				  } else {
				    offsetKeysMap.put(request, key);
				  }
				  key = new EtlKey();
//				 System.out.println(key.toString());
				}
//				reader.close();
			}
			for (Map.Entry<CamusRequest, EtlKey> entry : offsetKeysMap.entrySet()) {
				System.out.println("key:"+entry.getKey().toString()+"\t values:"+entry.getValue().toString());
			}
			System.out.println(offsetKeysMap.size());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
	}
	
}
