package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class SupplementMR extends MyMR {

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		boolean needSupplement = false, hasVideoLink = false, isVideo=false;
		String name = null;
		Map<String, String> supplementProp = new HashMap<String, String>();
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length==6){
				if( !tks[3].startsWith("人物_原名") 
						&& !tks[3].startsWith("别名相关性")
						&& !tks[3].startsWith("查询热度1")
						&& !tks[3].startsWith("TYPE重要性")
						&& !tks[3].startsWith("PAGEID")){
					needSupplement = true;
					name = tks[2];
				}else{
					supplementProp.put(tks[3], tks[4]);
					//delete original data from supplement dir
					inKv.values.set(i, null);
				}
				if(tks[3].startsWith("电影_")){
					isVideo = true;
				}
				if(tks[3].startsWith("电影_搜狗视频播放")){
					hasVideoLink = true;
				}
				/*
				if(tks[3].startsWith("电影_")||tks[3].startsWith("电视剧_")){
					isVideo = true;
				}
				if(tks[3].startsWith("电影_搜狗视频播放")||tks[3].startsWith("电视剧_搜狗视频播放")){
					hasVideoLink = true;
				}
				*/
			}
		}
		if(!needSupplement){
			inKv.values.clear();
		}else{
			Iterator it = supplementProp.keySet().iterator();
			while(it.hasNext()){
				String key = (String) it.next();
				String value = supplementProp.get(key);
				if(isVideo && !hasVideoLink && key.equals("TYPE重要性")){
					continue;
				}
				//MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t"+name+"\t"+key + "\t" + value+"\t-1", context);
				inKv.values.add(inKv.key+"\tSOGOUID_"+inKv.key+"\t"+name+"\t"+key + "\t" + value+"\t-1");
			}
		}
	}
}
