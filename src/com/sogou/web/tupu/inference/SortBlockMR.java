package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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

public class SortBlockMR extends MyMR {

	Map<String, String> mPropertyBlock = null;

	@Override
	public void myInitializerMapSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/block.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mPropertyBlock = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 2)
					continue;
				mPropertyBlock.put(seg[1], seg[0]);
			}
			br.close();
		} else {
			System.err.printf("load file %s err!\n", filePath);
			System.exit(1);
		}
	}

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
			if (tks.length == 6 && tks[1].equals("REFERENCE")) {
				String prop = tks[3].split("@")[0];
				if (mPropertyBlock.containsKey(prop)) {
					MyMR.mapOutput(mPropertyBlock.get(prop), "I_M_SB\t"
							+ tks[0], context);
				}
			}
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		Set<String> idSet = new HashSet<String>();
		int count = 0;
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			String [] tks = val.split("\t");
			if(tks.length == 2 && tks[0].equals("I_M_SB")){
				idSet.add(tks[1]);
				count ++;
			}
		}
		Iterator it = idSet.iterator();
		while(it.hasNext()){
			String id = (String) it.next();
			MyMR.reduceOutput(id, "I_R_SB\t"+inKv.key+"\t"+count, context);
		}
	}

	
	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		myInitializerMapSetup(conf);
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		String source = null, name = null;
		boolean manual = false;
		Map<String, Float> mPropertyCount = new HashMap<String, Float>();
		Map<String, Float> mTagCount = new HashMap<String, Float>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if (val == null)continue;
			String[] tks = val.split("\t");
			if(tks.length==4 && tks[1].equals("I_R_SB")){
				mTagCount.put(tks[2], Float.parseFloat(tks[3]));
				inKv.values.set(i, null);
			}
			if (tks.length == 6) {
				if (tks[1].contains("http") && source == null) {
					source = tks[1];
					name = tks[2];
				}
				if (tks[3].equals("BLOCKSEQ")) {
					manual = true;
				}
				if (tks[1].equals("REFERENCE")) {
					String[] tkstks = tks[3].split("@");
					if (mPropertyBlock.containsKey(tkstks[0])) {
						String prop = mPropertyBlock.get(tkstks[0]);
							if (mPropertyCount.containsKey(prop)) {
								Float count = mPropertyCount.get(prop);
								mPropertyCount.put(prop, count + 1);
							} else {
								mPropertyCount.put(prop, (float) 1);
							}
					}
				}
			}
		}
		if (source == null || manual || mTagCount.size()==0 ||mPropertyCount.size()==0)
			return;
		
		Iterator it = mPropertyCount.entrySet().iterator();
		
		while (it.hasNext()) {
			Entry entry = (Entry) it.next();
			String idprop = (String) entry.getKey();
			Float count = (Float) entry.getValue();
			if(mTagCount.containsKey(idprop)){
				Float tagcount = mTagCount.get(idprop);
				if (count == 1) {
					mPropertyCount.put(idprop, (float) (count /tagcount/ 10));
				} else {
					mPropertyCount.put(idprop, (float) (count /tagcount ));
				}
				
			}
		}
		List<Entry> list = new LinkedList<Entry>(mPropertyCount.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return -1
						* ((Comparable) ((Map.Entry) (o1)).getValue())
								.compareTo(((Map.Entry) (o2)).getValue());
			}
		});
		StringBuffer sbBlockSeq = new StringBuffer();
		for (int i = 0; i < list.size(); i++) {
			Map.Entry me = (Map.Entry) list.get(i);
			sbBlockSeq.append("|" + me.getKey() + "@"
					+ String.format("%.10f", me.getValue()));
		}
		if (sbBlockSeq.length() <= 0)
			return;
		if (source != null && !manual) {
			MyMR.reduceOutput(inKv.key, source + "\t" + name + "\tBLOCKSEQ\t"
					+ sbBlockSeq + "\tNULL", context);
		}
	}

}
