package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class RankListMR extends MyMR {

	Set<String> propSet = null;
	@Override
	public void myInitializerReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/type.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			propSet = new HashSet<String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length<1)
					continue;
				propSet.add(seg[0]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		for(int i = 0; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			String [] tks = val.split("\t");
			if(tks.length==6){
				if(propSet.contains(tks[3]) || tks[3].startsWith("网络小说") ){
					MyMR.reduceOutput(null, val, context);
				}
			}
		}
	}

	@Override
	public void myInferenceMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if(inKv.values.size()==1){
			String val = inKv.values.get(0);
			String [] tks = val.split("\t");
			if(tks.length==7){
					if(tks[4].equals("网络小说_首发网站")){
						outKv.key = inKv.key;
						outKv.values.add(val);
					}
			}
		}
	}

}
