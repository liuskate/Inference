package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class WordPvMR extends MyMR {

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			String[] tks = val.split("\t");
			if (tks.length == 6) {
				MyMR.mapOutput(tks[2], "I_PV_M\t"+ tks[0], context);
				if(tks[1].startsWith("http://"))
					MyMR.mapOutput(tks[1], "I_SR_M\t"+ tks[0], context);
			}
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		String name = inKv.key;
		Integer pv = 0, sogourank=0;
		HashSet<String> sourceIds = new HashSet<String>();
		HashSet<String> sourceIdsSR = new HashSet<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String value = inKv.values.get(i);
			if (value.startsWith("I_PV_M")) {
				String[] tks = value.split("\t");
				if (tks.length != 2)
					continue;
				if (sourceIds.contains(tks[1]))
					continue;
				sourceIds.add(tks[1]);
			}else if(value.contains("\tPV\t")){
				String[] tks = value.split("\t");
				if(tks.length==3){
					try{
						pv = Integer.parseInt(tks[2]);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}else if (value.startsWith("I_SR_M")) {
				String[] tks = value.split("\t");
				if (tks.length != 2)
					continue;
				if (sourceIdsSR.contains(tks[1]))
					continue;
				sourceIdsSR.add(tks[1]);
			}else if(value.contains("\tSOGOURANK\t")){
				String[] tks = value.split("\t");
				if(tks.length==3){
					try{
						sogourank += Integer.parseInt(tks[2]);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		}
		for (String sourceId : sourceIds) {
			if(pv > 2)
				MyMR.reduceOutput(sourceId, "I_PV_R\t"+ pv, context);
		}
		if(sogourank < 2) {
			return;
		}
		for (String sourceId : sourceIdsSR) {
				MyMR.reduceOutput(sourceId, "I_SR_R\t"+inKv.key+"\t"+ sogourank, context);
		}
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		String name = null;
		String pv = null, pvmanual = null, pvset = null; 
		Float pvweight = null;
		boolean black = false, isTVShow = false;
		Integer weeklyPv = 0, sogourank = 0, sogourank_iqiyi=0; 
		for(int i = 0; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length== 6){
				if(tks[3].equals("查询热度")){
					pvmanual = tks[4];
				}
				if(name == null){
					name = tks[2];
				}
				if(tks[3].contains("电视节目_")){
					isTVShow = true;
				}
			}else if(tks.length==2 && tks[1].equals("BLACK_PV")){
				black = true;
				inKv.values.set(i, null);
			}else if(tks.length==3 && tks[1].equals("SET_PV")){
				pvset = tks[2];
				inKv.values.set(i, null);
			}else if(tks.length==3 && tks[1].equals("WEIGHT_PV")){
				pvweight = Float.parseFloat(tks[2]);
				inKv.values.set(i, null);
			}else if(tks.length==3 && tks[1].equals("I_PV_R")){
				weeklyPv += Integer.parseInt(tks[2]);
				inKv.values.set(i, null);
			}else if(tks.length==4 && tks[1].equals("I_SR_R")){
				sogourank += Integer.parseInt(tks[3]);
				if(tks[2].startsWith("http://www.iqiyi.com/lib"))
					sogourank_iqiyi += Integer.parseInt(tks[3]);
				inKv.values.set(i, null);
			}
		}
		if(weeklyPv>1){
			MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t" +name+"\t查询热度0\t"+ String.format("%d", weeklyPv) +"\t-1", context);
		}
		if(sogourank>1){
			if(isTVShow){
				MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t" +name+"\t查询热度1\t"+ String.format("%d", sogourank_iqiyi) +"\t-1", context);
			}else
				MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t" +name+"\t查询热度1\t"+ String.format("%d", sogourank) +"\t-1", context);
		}
		if(black)return;
		if(pvmanual!=null){
			pv = pvmanual;
		}else
			pv = pvset;
		if(pv==null)return;
		if(pvweight!=null){
			Float weightPv = Float.parseFloat(pv)* pvweight;
			MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t" +name+"\t查询热度\t"+ String.format("%.5f", weightPv) +"\t-1", context);
		}else{
			MyMR.reduceOutput(inKv.key, "SOGOUID_"+inKv.key+"\t" +name+"\t查询热度\t"+ pv +"\t-1", context);
		}
	}

}
