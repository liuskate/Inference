package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class ImportanceMR extends MyMR {

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv, TaskInputOutputContext context) throws IOException, InterruptedException {
		Float manualImportance = null;
		Float pagerankImportance = null;
		String source = null, name = null;
		for(int i = 0; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
			if(tks.length == 3 && val.contains("PAGERANK_IMPORTANCE")){
				inKv.values.set(i,  null);
				pagerankImportance = Float.parseFloat(tks[2]);
			}else if(tks.length == 6){
				if(tks[3].equals("重要性")){
					manualImportance = Float.MAX_VALUE;
				}
				if(tks[1].contains("http") && source == null){
					source = tks[1];
					name = tks[2];
				}
			}
		}
		if(source!=null && pagerankImportance!=null && manualImportance == null){
			//pagerankImportance = (float) (10000*(pagerankImportance-0.150000)/(2867.600000-0.150000));
			outKv.key = inKv.key;
			outKv.values.add(source + "\t" + name + "\t重要性\t" + String.format("%.4f",pagerankImportance) + "\tNULL");
		}
	}
	
}
