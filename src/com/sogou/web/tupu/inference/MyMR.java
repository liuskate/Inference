package com.sogou.web.tupu.inference;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Tool;

public class MyMR {

	public static String getConfigureValue(Configuration conf, String name){
		String unspaced = conf.get(name).replaceAll("\\s+", "");
		return unspaced;
	}
	public static void mapOutput(String key, String value,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if(value== null)return;
		if(value.length()==0)return;
		if (key == null) {
			context.write(null, new Text(value));
		} else {
			if(key.length()>0)
				context.write(new Text(key), new Text(value));
		}
	}

	public static void reduceOutput(String key, String value,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		//if(true) return;
		if(value == null)return;
		if(value.length()==0)return;
		//value = value+"\t$";
		if(key==null){
			context.write(null, new Text(value.getBytes("GBK")));
		}else{
			if(key.length()>0 ){
				context.write(new Text(key.getBytes("GBK")),
					new Text(value.getBytes("GBK")));
			}
		}
	}

	public void myInitializerMapSetup(Configuration conf) throws IOException {
	}

	public void myInitializerReduceSetup(Configuration conf) throws IOException {
	}

	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
	}

	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
	}

	public void myInferenceMapSetup(Configuration conf) throws IOException {
	}

	public void myInferenceReduceSetup(Configuration conf) throws IOException {
	}

	public void myInferenceMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
	}

	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
	}
	public static void main(String[] args){
		Tool tool = new Initializer();
		Configuration conf = HBaseConfiguration.create();
		Main.getConf(conf, args);
		System.err.println(MyMR.getConfigureValue(conf, "fs.path.initializer.inputs"));
	}
}
