package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;


public class IdInferenceMR extends MyMR{
	
	Map<String,String> mSchemaAlias=null;
	
	@Override
	public void myInitializerMapSetup(Configuration conf) throws IOException{
		mSchemaAlias = new HashMap<String, String>();
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/inf.schema.id";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length!=2)
					continue;
				mSchemaAlias.put(seg[0], seg[0]);
				mSchemaAlias.put(seg[1], seg[0]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
	}
	
	// find links need to inference
	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv, TaskInputOutputContext context) throws IOException, InterruptedException{
		if(inKv.values == null)return;
		String line = inKv.values.get(0);
		String[] seg=line.split("\t");
		if(seg.length!=6)return;
		String [] tkstks = seg[3].split("@");
		if(tkstks.length>0){
			if(mSchemaAlias.containsKey(tkstks[0]) && !seg[5].equals("-1")){
				MyMR.mapOutput(seg[5], "M_M_I\t"+seg[0], context);
			}
		}
	}
	
	
	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv, TaskInputOutputContext context) throws IOException, InterruptedException{
		Set<String> sourceIds = new HashSet<String>();
		Set<String> sAlias = new HashSet<String>();
		for(int i = 0; i<inKv.values.size(); i++){
			String value = inKv.values.get(i);
			String [] tks = value.split("\t");
			if(value.startsWith("M_M_I")){
				if(tks.length!=2)continue;
				sourceIds.add(tks[1]);
			}else if(tks.length==3){
				sAlias.add(tks[1]);
				sAlias.add(tks[2]);
			}
		}
		String alias = "";
		Iterator it = sAlias.iterator();
		while(it.hasNext()){
			alias+="\t"+it.next();
		}
		alias = alias.trim();
		for(String sourceId : sourceIds){
				MyMR.reduceOutput(sourceId, "M_R_I\t" + inKv.key +"\t"+alias, context);
		}
	}
	
	 
	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		myInitializerMapSetup(conf);
	}
	
	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv, TaskInputOutputContext context) {
		Set<String> addedSet = new HashSet<String>();
		Set<String> addedID = new HashSet<String>();
		Map<String, String> mIdAlias = new HashMap<String, String>(); 
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val==null)continue;
			String [] tks = val.split("\t");
			if(tks.length==6){
				String [] tkstks = tks[3].split("@");
				if(mSchemaAlias.containsKey(tkstks[0]) && !tks[5].equals("-1")){
					String k = mSchemaAlias.get(tkstks[0])+"\t"+tks[4];
					if(!addedSet.contains(k)){
						addedSet.add(k);
						addedID.add(k+"\t"+tks[5]);
					}
				}
			}
			if(tks.length>=4){
				if(val.contains("M_R_I\t")){
					mIdAlias.put(tks[2], val);
				}
			}
		}
		Map<String, String> mAliasId = new HashMap<String, String>();
		Iterator it = addedID.iterator();
		while(it.hasNext()){
			String line = (String) it.next();
			String [] tks = line.split("\t");
			if(tks.length==3){
				if(mIdAlias.containsKey(tks[2])){
					String [] aliasLst = mIdAlias.get(tks[2]).split("\t");
					for(int i = 3 ; i < aliasLst.length; i++){
						mAliasId.put(tks[0]+"\t"+aliasLst[i], tks[2]);
					}
				}
			}
		}
		
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length==6){
				String [] tkstks = tks[3].split("@");
				if(mSchemaAlias.containsKey(tkstks[0]) && tks[5].equals("-1")){
					String k = mSchemaAlias.get(tkstks[0])+"\t"+tks[4];
					String id1 = "-1";
					if(mAliasId.containsKey(k)){
						id1 = mAliasId.get(k);
					}
					inKv.values.set(i, tks[0]+"\t"+tks[1]+"\t"+tks[2]+"\t"+tks[3]+"\t"+tks[4]+"\t"+id1);
				}
			}
			if(val.contains("M_R_I\t")){
				//mIdAlias.put(tks[1], val);
				inKv.values.set(i, null);
			}
		}
		
	}
}