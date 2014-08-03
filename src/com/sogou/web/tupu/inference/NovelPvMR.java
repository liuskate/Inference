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

public class NovelPvMR extends MyMR {

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		String name = null;
		boolean isNovel = false, isQidian = false;
		Float pv = (float) 0.0, click=(float) 0.0;
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length==6){
				if(name == null){
					name = tks[2];
				}
				if(tks[3].contains("小说_")){
					isNovel = true;
				}
				if(tks[1].contains("http://www.qidian.com/book")||
						tks[1].contains("http://www.qdmm.com/mmweb")){
					isQidian = true;
				}
				if(tks[3].equals("查询热度0")){
					try{
						pv = (float) Integer.parseInt(tks[4]);
					}catch(Exception e){
					}
				}
				if(tks[3].equals("点击")){
					tks[4] = tks[4].replaceAll(",", "");
					try{
						click = (float) Integer.parseInt(tks[4]);
					}catch(Exception e){
					}
				}
			}
		}
		if(isNovel){
			for(int i = 0 ; i<inKv.values.size(); i++){
				String val = inKv.values.get(i);
				if(val == null)continue;
				String [] tks = val.split("\t");
				if(tks.length==6){
					if(tks[3].equals("查询热度1") || tks[3].equals("重要性")){
						inKv.values.set(i, null);
					}
				}
			}
			if(isQidian && click > 2000000){
				MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t"+name+"\t查询热度1\t" + String.format("%.4f", pv/1000)+"\t-1", context);
				MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t"+name+"\t重要性\t" + String.format("%.4f", pv/10000)+"\t-1", context);
			}
		}
		
	}

}
