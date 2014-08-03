package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class CategoryMR extends MyMR {

	Map<String, Integer> mSummary = null;
	Map<String, String> mTP = null;
	Map<String, String> mCate = null;
	Map<String, String> mSchema = null;
	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/summary.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			mSummary = new HashMap<String,Integer>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length!=1)
					continue;
				mSummary.put(seg[0], 1);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
		
		filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/cate.conf";
		configPath = new Path(filePath);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			mTP = new HashMap<String,String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length!=3)
					continue;
				mTP.put(seg[0], seg[2]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
		
		filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/cate.priority";
		configPath = new Path(filePath);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			mCate = new HashMap<String,String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length<2)
					continue;
				mCate.put(seg[0]+"\t"+seg[1], seg[0]);
				mCate.put(seg[1]+"\t"+seg[0], seg[0]);
			}
			br.close();
		}
		else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
		filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/cate.schema";
		configPath = new Path(filePath);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			mSchema = new HashMap<String,String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length<2)
					continue;
				mSchema.put(seg[1], seg[0]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		int hasCombine = 0;
		Map<String, Integer> mSchemaNum = new HashMap<String, Integer>();
		String name = "";
		String righttype = null;
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val == null)continue;
//System.err.println("line:" + val);
			String[] tks = val.split("\t");
			if (tks.length == 6) {
				String[] tkstks = tks[3].split("@");
				String[] tkstks1 = tkstks[0].split(":");
				if (mSummary.containsKey(tkstks1[0])
						|| (tkstks1.length == 2 && mSummary
								.containsKey(tkstks1[1]))) {
					String type = "通用";
					String[] tkstks2 = tkstks1[0].split("_");
					if (tkstks2.length > 1) {
						type = tkstks2[tkstks2.length - 2];
					}
					if (tkstks1[0].equals("子概念")) {
						hasCombine = 1;
					}
					if(!type.equals("通用")){
						if(mSchemaNum.containsKey(type + "\t" + tkstks1[0])){
							mSchemaNum.put(type + "\t" + tkstks1[0], 1+mSchemaNum.get(type + "\t" + tkstks1[0]));
						}else
							mSchemaNum.put(type + "\t" + tkstks1[0], 1);
					}
				}
				name = tks[2];
				if(tks[3].equals("CATSEQ")&&(tks[4].contains("music_person")||
											tks[4].contains("music_music"))){
					righttype = "单曲";
				}
			}
		}
//System.err.println("hasCombine=" + hasCombine + ",name=" + name+",righttype="+righttype);		
		Iterator it = (Iterator) mSchemaNum.keySet().iterator();
		Map<String, Integer> mTPL1 = new HashMap<String, Integer>();
		String ot = null;
		while(it.hasNext()){
			String key = (String) it.next();
//System.err.println("mSchemaNum:key="+key);	
			String [] tks = key.split("\t");
			if(mTP.containsKey(tks[0])){
				if(mTPL1.containsKey(mTP.get(tks[0]))){
					Integer n = mTPL1.get(mTP.get(tks[0]));
					mTPL1.put(mTP.get(tks[0]), n + 1);
				}else
					mTPL1.put(mTP.get(tks[0]),  1);
			}
			if(mSchema.containsKey(tks[1])){
				ot = mSchema.get(tks[1]);
			}
		}
		
		Map<String, String> mType = new HashMap<String, String>();
		if(ot!=null && mTPL1.size()>0){
			mType.put(ot, "");
		}else{
			List list = new LinkedList(mTPL1.entrySet());
			Collections.sort(list, new Comparator() {
				public int compare(Object o1, Object o2) {
					return -1*((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
				}
			});
			int ok = 0;
			for(int i = 0 ; i<list.size(); i++){
				Map.Entry mei = (Entry) list.get(i);
				String ki = (String) mei.getKey();
				Integer vi = (Integer) mei.getValue();
				for(int j = i +1; j<list.size(); j++){
					Map.Entry mej = (Entry) list.get(j);
					String kj = (String) mej.getKey();
					Integer vj = (Integer) mej.getValue();
					if(mCate.containsKey(ki+"\t"+kj) && vi>=2&& vj>=2){
						ok = 1;
						mType.put(mCate.get(ki+"\t"+kj), "");
						break;
					}
				}
				if(ok==1)break;
			}
			if(ok!=1 && list.size()>0)
				mType.put((String)((Entry)list.get(0)).getKey(), "");
		}
		boolean hasGroup = false;
		if(hasCombine==1 && mType.size()>0){
			hasCombine = 2;
		}
		if(hasCombine == 1){
			hasGroup = true;
		}
		it = (Iterator) mType.keySet().iterator();
		String type = "";
		while(it.hasNext()){
			type = (String) it.next();
			if(type.equals("单曲")){
				type = "1";
			}
		}
		if(hasGroup) type = "组合概念";
		if(type.equals("1") && righttype!=null){
			type = righttype;
		}
//System.err.println("hasGroup="+hasGroup + ", type ="+type + ", righttype = "+ righttype);	
		if(!type.equals("1") && !type.equals("")){
			MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t"+name+"\t所属分类\t"+type+"\t-1", context);
		}
	}
	
	public static void main(String[] argv){
		int i= 5;
		if(i<10){
			System.out.println("10");
		}else if(i<20){
			System.out.println("20");
		}
	}
}
