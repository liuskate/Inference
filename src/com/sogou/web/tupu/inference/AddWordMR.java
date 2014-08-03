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
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class AddWordMR extends MyMR {

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			String[] tks = val.split("\t");
			if (tks.length == 6) {
				MyMR.mapOutput("AN_" + tks[2], "I_AN_M\t"+ tks[0], context);
			}
		}
		// inKv.values.clear();
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if (!inKv.key.startsWith("AN_"))
			return;
		String name = inKv.key.substring(3);
		int count = 0;
		HashSet<String> sourceIds = new HashSet<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String value = inKv.values.get(i);
			if (value.startsWith("I_AN_M")) {
				String[] tks = value.split("\t");
				if (tks.length != 2)
					continue;
				if (sourceIds.contains(tks[1]))
					continue;
				sourceIds.add(tks[1]);
				count++;
			}
		}
		if (count == 1)
			return;
		for (String sourceId : sourceIds) {
			MyMR.reduceOutput(sourceId, "I_AN_R\t" + name + "\t" + count, context);
		}
	}

	Map<String, String> mBlock = null;
	Map<String, Integer> mBlockVal = null;

	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/seq.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mBlock = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 2)
					continue;
				mBlock.put(seg[0], seg[1]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}

		filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/normlst";
		configPath = new Path(filePath);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mBlockVal = new HashMap<String, Integer>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 3)
					continue;
				mBlockVal.put(seg[0] + "\t" + seg[1], Integer.parseInt(seg[2]));
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
		String name = null, type = null;
		List<String> manualWords = new ArrayList<String>();
		Float importance = null;
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val == null) continue;
			String[] tks = val.split("\t");
			if (name == null && tks.length == 4 && tks[1].equals("I_AN_R")) {
				name = tks[2];
				inKv.values.set(i, null);
			} else if (tks.length == 6) {
				if (tks[3].equals("ADDNAME")) {
					return;
				}
				
				String[] tkstks = tks[3].split("_");
				if(tkstks.length > 1) {
					type = tkstks[0];
				}
				if (importance == null && tks[3].equals("重要性")) {
					importance = Float.parseFloat(tks[4]);
				}
			} else if (tks.length == 3 && tks[1].equals("ADDNAME")) {
				manualWords.add(tks[2]);
				inKv.values.set(i, null);
			}
		}
		for (int i = 0; i<manualWords.size(); i++) {
			MyMR.reduceOutput(inKv.key, "REFERENCE\t" + name + "\tADDNAME\t" + manualWords.get(i) + "\t-1", context);
		}
		if(manualWords.size()>0) return;
		if (name == null || type == null)
			return;
		if (importance == null) {
			for (int i = 0; i < outKv.values.size(); i++) {
				String val = outKv.values.get(i);
				String[] tks = val.split("\t");
				if (tks.length == 5 && tks[2].equals("重要性")) {
					importance = Float.parseFloat(tks[3]);
				}
			}
		}
		if (importance == null)
			importance = (float) 0.0;
		if ((type.equals("人物") && importance > 30.0) || importance > 100.0)
			return;
//		TreeMap<String, Float> mAddWord = new TreeMap<String, Float>(
//				new Comparator<String>(){
//					@Override
//					public int compare(String o1, String o2) {
//						return -1*o1.compareTo(o2);
//					}
//				}
//				);
		TreeMap<String, Float> mAddWord = new TreeMap<String, Float>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String tmpType = null;
			Float tmpWeight = null;
			String val = inKv.values.get(i);
			if(val == null)continue;
			String[] tks = val.split("\t");
			if (tks.length == 6) {
				String[] tkstks = tks[3].split("_");
				if (tkstks[0].equals(type) && mBlock.containsKey(tks[3])) {
					String sblock = mBlock.get(tks[3]);
					String[] tkstks1 = sblock.split("@");
					if (tkstks1.length == 2) {
						tmpType = tkstks1[1];
						tmpWeight = Float.parseFloat(tkstks1[0]);
					} else {
						int maxinfo = 0;
						String maxstr = tks[3] + "\t" + tks[4];
						for (int j = 0; j < tks[4].length(); j++) {
							for (int k = 2; k <= 4 && j + k <= tks[4].length(); k++) {
								String str = tks[3] + "\t"
										+ tks[4].substring(j, j + k);
								if (mBlockVal.containsKey(str)) {
									Integer tmpinfo = mBlockVal.get(str);
									if (tmpinfo > maxinfo) {
										maxstr = str;
										maxinfo = tmpinfo;
									}
								}
							}
						}
						tkstks1 = maxstr.split("\t");
						if (tkstks1.length < 2)
							tmpType = "";
						else
							tmpType = tkstks1[1];
						tmpWeight = Float.parseFloat(sblock);
					}
					int pos = tmpType.indexOf("-");
					if (pos > 0 && pos + 1 < tmpType.length()
							&& isNumeric(tmpType.substring(0, pos))
							&& Character.isDigit(tmpType.charAt(pos + 1))) {
						tmpType = tmpType.substring(0, pos);
					}
//System.err.printf("%s %s\t%s: type = %s, weight = %f\n", inKv.key, tks[3], sblock, tmpType, tmpWeight);
					if(mAddWord.containsKey(tmpType)) {
						if(tmpWeight < mAddWord.get(tmpType)){
							mAddWord.put(tmpType, tmpWeight);
						}
					}else{
						mAddWord.put(tmpType, tmpWeight);
					}
				}
			}
		}
		Iterator it = mAddWord.keySet().iterator();
		List<Map.Entry> list = new ArrayList<Map.Entry>();
		while(it.hasNext()){
			String str = (String) it.next();
			list.add(new AbstractMap.SimpleEntry(str, mAddWord.get(str)));
		}
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
			}
		});
		for(int i = 0 ; i<list.size(); i++){
			Entry entry = (Entry) list.get(i);
			String toadd = (String) entry.getKey();
			MyMR.reduceOutput(inKv.key, "REFERENCE\t" + name + "\tADDNAME\t" + toadd + "\t-1", context);
			break;
		}
	}

	public static boolean isNumeric(String str) {
		for (int i = str.length(); --i >= 0;) {
			if (!Character.isDigit(str.charAt(i))) {
				return false;
			}
		}
		return true;
	}

}
