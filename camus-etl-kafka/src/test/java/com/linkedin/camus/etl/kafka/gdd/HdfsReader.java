package com.linkedin.camus.etl.kafka.gdd;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
public class HdfsReader {
	
	private static final String CODE_CLASS = "org.apache.hadoop.io.compress.DefaultCodec";
	static {System.setProperty("hadoop.home.dir","E:/kuaidi/hdfs-storm/hadoop-common-2.2.0-bin-master");}
	public static void main(String[] args) {
	/*	FSDataInputStream in  = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			in = fs.open(new Path("file:////E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/11/gdd.102183.0.4.3.1431273600000.deflate"));
            byte[] buffer = new byte[8];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                System.out.println(new String(buffer));
            }
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
		}
		*/
		  try {
	            Configuration conf = new Configuration();
	            FileSystem fs = FileSystem.get(URI.create("hdfs://10.0.50.10:8020/work/guodongdong/kafkamq/data/test/daily/2015/05/17/"),conf);
//	            Path file = new Path("file:///E:/kuaidi/warehouse/trunk/vip/hive/camus/source/camus/rocketmq/11/gdd.102183.0.4.3.1431273600000.deflate");
	            Path file = new Path("hdfs://10.0.50.10:8020/work/guodongdong/kafkamq/data/test/daily/2015/05/17/");
//	            Path file = new Path("hdfs://10.0.50.10:8020/work/guodongdong/rocketmq/data/gdd/daily/2015/05/11/");
	            for (FileStatus f : fs.listStatus(file)) {
	            	System.out.println("path:"+f.getPath().toString());
	            	  FSDataInputStream getIt = fs.open(f.getPath());
	  	            Class<?> codecClass = Class.forName(CODE_CLASS);
	  		        CompressionCodec codec = (CompressionCodec) ReflectionUtils
	  		                .newInstance(codecClass, conf);
	  		        InputStream stream = codec.createInputStream(getIt);
	  	            BufferedReader d = new BufferedReader(new InputStreamReader(stream));
	  	            String s = "";
	  	            while ((s = d.readLine()) != null) {
	  	                System.out.println(s);
	  	            }
	  	            d.close();
	            }
	            
	          
	            
	            fs.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
//		System.out.println("gdd");
	}
	public static void main333(String[] args) {
		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir","E:/kuaidi/hdfs-storm/hadoop-common-2.2.0-bin-master");
//		conf.set("fs.defaultFS", "hdfs://10.0.50.13:8020/user/test/camus-master");
//		conf.set("fs.defaultFS", "hdfs://10.2.3.10:9000/user/test/camus-master/camus-example/target/1");
//		FileSystem fs = FileSystem.get(conf);
		
	/*	  OutputStream out = new FileOutputStream("D:/1822"); 
		  byte[] ioBuffer = new byte[1024];
		  int readLen = hdfsInStream.read(ioBuffer);
		  while(-1 != readLen){
		  out.write(ioBuffer, 0, readLen);  
		  readLen = hdfsInStream.read(ioBuffer);
		  }
		  out.close();
		  hdfsInStream.close();
	*/
		FSDataInputStream in =  null;
		 try{ 
			 String hdfsUrl = "hdfs://bj-bigdata-spark00:9000//user/test/gdd/etl/dwd_order_flow/1428742080455.data";
			 FileSystem fs = FileSystem.get(URI.create(hdfsUrl),conf);
			 in = fs.open(new Path(hdfsUrl));
	            //实验一：输出全部文件内容 
	            System.out.println("实验一：输出全部文件内容"); 
	            //让FileSystem打开一个uri对应的FSDataInputStream文件输入流，读取这个文件 
	            in = fs.open( new Path(hdfsUrl) ); 
	            byte[] buffer = new byte[8];
	            int bytesRead;
	            while ((bytesRead = in.read(buffer)) > 0) {
	                System.out.println(buffer);
	            }
	        }catch(Exception ex){
	        	ex.printStackTrace();
	        }finally{ 
	            IOUtils.closeStream(in); 
	        } 
//    fs.createNewFile(new Path("/user/hbase/test"));
		/*FileStatus[] status = fs.listStatus(new Path("/user/test/camus-master"));
		for(int i=0;i<status.length;i++){
			System.out.println(status[i]);
		    System.out.println(status[i].getPath());
		}*/
	}
}
