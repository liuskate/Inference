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

public class FoodEffectMR extends MyMR {

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			String[] tks = val.split("\t");
			if (tks.length == 6 && tks[3].contains("食材_功效标签:功效标签属性_权值")) {
				MyMR.mapOutput("FD_EF_ALL", "I_FE_M_W\t"+ val, context);
			} else if (tks.length == 6 && tks[3].contains("食材_功效标签:功效标签属性_功效名称")) {
				MyMR.mapOutput("FD_EF_ALL", "I_FE_M_T\t"+ val, context);
			}
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if (!inKv.key.equals("FD_EF_ALL"))
			return;
		Map<String, String> weight = null, tag = null, name = null, allTag;
		weight = new HashMap<String, String>();
		tag = new HashMap<String, String>();
		allTag = new HashMap<String, String>();
		name = new HashMap<String, String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			String[] tks = val.split("\t");
			if (tks.length == 7) {
				String[] tkstks = tks[4].split("@");
				if (tkstks.length == 2) {
					if (val.startsWith("I_FE_M_W")) {
						weight.put(tks[1] + "\t" + tkstks[1], tks[5]);
					} else if (val.startsWith("I_FE_M_T")) {
						tag.put(tks[1] + "\t" + tkstks[1], tks[5]);
						name.put(tks[1] + "\t" + tkstks[1], tks[1] + "\t"
								+ tks[2] + "\t" + tks[3]);
						allTag.put(tks[5], "");
					}
				}
			}
		}
		Map<String, ArrayList<String>> tagMap = new HashMap<String, ArrayList<String>>();
		Iterator it = name.keySet().iterator();
		for (; it.hasNext();) {
			String nKey = (String) it.next();
			String nVal = name.get(nKey);
			if (weight.containsKey(nKey) && tag.containsKey(nKey)) {
				String wVal = weight.get(nKey);
				String tVal = tag.get(nKey);
				if (tagMap.containsKey(tVal)) {
					List<String> lst = tagMap.get(tVal);
					lst.add(-1 * Integer.parseInt(wVal) + "\t" + nVal + "\t"
							+ tVal);
				} else {
					List<String> lst = new ArrayList<String>();
					lst.add(-1 * Integer.parseInt(wVal) + "\t" + nVal + "\t"
							+ tVal);
					tagMap.put(tVal, (ArrayList<String>) lst);
				}
				Iterator itAT = allTag.keySet().iterator();
				while (itAT.hasNext()) {
					String atKey = (String) itAT.next();
					if (!atKey.equals(tVal) && tVal.contains(atKey)) {
						if (tagMap.containsKey(atKey)) {
							List<String> lst = tagMap.get(atKey);
							lst.add(-1 * Integer.parseInt(wVal) + "\t" + nVal
									+ "\t" + atKey);
						} else {
							List<String> lst = new ArrayList<String>();
							lst.add(-1 * Integer.parseInt(wVal) + "\t" + nVal
									+ "\t" + atKey);
							tagMap.put(atKey, (ArrayList<String>) lst);
						}
					}
				}
			}
		}
		Iterator itTM = tagMap.keySet().iterator();
		int nID = 60000000;
		int count = 1;
		for (; itTM.hasNext();) {
			String tagname = (String) itTM.next();
			List<String> effList = tagMap.get(tagname);
			Collections.sort(effList, new Comparator() {
				public int compare(Object o1, Object o2) {
					String s1 = ((String) o1);
					String s2 = ((String) o2);
					int n1 = Integer.parseInt(s1.substring(0, s1.indexOf("\t")));
					int n2 = Integer.parseInt(s2.substring(0, s2.indexOf("\t")));
					return n1 - n2;
				}
			});
			for (int i = 0; i < effList.size() && i < 100; i++) {
				String val = effList.get(i);
				String[] tks = val.split("\t");
				if (tks.length == 5 && tks[4].length() > 0) {
					count++;
					MyMR.reduceOutput(nID + "", "RANKLIST_" + nID
							+ "\t#" + tks[4] + "食材#\t"
							+ "排行榜:榜单信息属性_名称@" + count + "\t" + tks[3]
							+ "\t" + tks[1], context);
					MyMR.reduceOutput(nID + "", "RANKLIST_" + nID + "\t#" + tks[4] + "食材#\t"
							+ "排行榜:榜单信息属性_排名@" + count + "\t"
							+ (i+1) + "\t-1", context);
				}
			}
			nID++;
		}
	}

}
