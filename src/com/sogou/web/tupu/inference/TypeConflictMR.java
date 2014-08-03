package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class TypeConflictMR extends MyMR {

	Map<String, Integer> mCat = null, mCK = null;
	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/type.check.all";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(configPath) && fs.isFile(configPath)){
			mCat = new HashMap<String,Integer>();
			mCK = new HashMap<String,Integer>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] seg=line.split("\t");
				if(seg.length!=3)
					continue;
				mCat.put(seg[1]+"\t"+seg[2], 1);
				mCat.put(seg[2]+"\t"+seg[1], 1);
				mCat.put(seg[1], 1);
				mCat.put(seg[2], 1);
				mCK.put(seg[1]+"\t"+seg[2], 1);
				mCK.put(seg[2]+"\t"+seg[1], 1);
			}
			br.close();
		}
	}

	public static int compare(String s1, String s2, int nfield){
		String [] tks1 = s1.split("\t");
		String [] tks2 = s2.split("\t");
		if(tks1.length<nfield+1 || tks2.length<nfield+1){
			return 0;
		}
		return Integer.parseInt(tks1[nfield]) - Integer.parseInt(tks2[nfield]);
	}
	
	
	public static void bubleSort(List<String> lst, int nfield, int start, int end){
//System.err.println("sort: at " + nfield + ", from " + start + " to " + end);
		for(int i = start ; i<end; i++){
			for(int j = i ; j<end-i; j++){
				if(compare(lst.get(i), lst.get(j), nfield) < 0){
					String tmp = lst.get(i);
					lst.set(i, lst.get(j));
					lst.set(j, tmp);
				}
			}
		}
	}
	
	public static void bubleSecondSortBy(List<String> lst, int nfieldKey, int nfieldValue){
		String key = "";
		int start = 0, end = 0;
		for(int i = 0 ; i<lst.size(); i++){
			String[] tks = lst.get(i).split("\t");
			if(tks.length<nfieldKey || tks.length <nfieldValue)continue;
			if(key.equals("")){
				start  = i;
				key = tks[nfieldKey];
			}else if(key.equals(tks[nfieldKey])){
				end = i+1;
			}else{
				end = i;
				bubleSort(lst, nfieldValue, start, end);
				key = tks[nfieldKey];
				start = i;
				end = -1;
			}
		}
		bubleSort(lst, nfieldValue, start, end);
	}
	
	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		Map<String, Integer> addedMap = new HashMap<String,Integer>();
		for(int i = 0 ; i < inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length == 6){
				String [] tkstks = tks[3].split(":");
				tkstks = tkstks[0].split("_");
				if(tkstks.length == 1)continue;
				addedMap.put(tkstks[tkstks.length-2], 1);
			}
		}
		List<String> lstCat = new ArrayList<String>();
		Iterator it = (Iterator) addedMap.keySet().iterator();
		while(it.hasNext()){
			String cstr = (String) it.next();
			if(mCat.containsKey(cstr)){
				lstCat.add(cstr);
			}
		}
		addedMap.clear();
		Map<String, Integer> mSuspect = addedMap;
		for(int i = 0; i<lstCat.size(); i++){
			for(int j = i + 1; j<lstCat.size(); j++){
				if(mCat.containsKey(lstCat.get(i)+"\t"+lstCat.get(j))){
					mSuspect.put(lstCat.get(i), 1);
					mSuspect.put(lstCat.get(j), 1);
				}
			}
		}
		Map<String, Integer> mHit = new HashMap<String,Integer>();
		Map<String, Integer> cat1 = new HashMap<String,Integer>();
		Map<String, Integer> cat = new HashMap<String,Integer>();
		for(int i = 0 ; i < inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length == 6){
				String [] tkstks = tks[3].split(":");
				String [] tkstks1 = tkstks[0].split("_");
				if(tkstks1.length == 1)continue;
				if(mSuspect.containsKey(tkstks1[tkstks1.length-2])){
					String tp = tkstks1[tkstks1.length - 2];
					String source = "", url = tks[1];
					if(tks[1].contains("http")){
						tkstks1 = tks[1].split("/");
						if(tkstks1.length>=3){
							source = tkstks1[2];
						}else
							source = "";
					}else if(tks[1].contains("_")){
						tkstks1 = tks[1].split("_");
						if(tkstks1.length>=1){
							source = tkstks1[0];
						}else
							source = "";
					}else{
						source = "REFERENCE";
						url = "SOGOUID_" + tks[0];
					}
					String key = tp+"\t"+source+"\t"+url+"\t"+tks[2];
					if(!mHit.containsKey(tks[1]+"\t"+tkstks[0])){
						mHit.put(url+"\t"+tkstks[0], 1);
						if(cat1.containsKey(key)){
							Integer n = 1 + cat1.get(key);
							cat1.put(key, n);
						}else{
							cat1.put(key, 1);
						}
					}
					if(cat.containsKey(key)){
						Integer n = 1 + cat.get(key);
						cat.put(key, n);
					}else{
						cat.put(key, 1);
					}
				}
			}
		}
		mHit.clear();
		Map<String, Integer> mId = new HashMap<String,Integer>();
		Map<String, Integer> mIdTypeStat1 = new HashMap<String,Integer>();
		Map<String, Integer> mIdTypeStat2 = new HashMap<String,Integer>();
		it = (Iterator) cat.keySet().iterator();
		String name = null;
		while(it.hasNext()){
			String k = (String) it.next();
			String [] tks = k.split("\t");
			if(tks.length!=4)continue;
			name = tks[3];
			if(mIdTypeStat1.containsKey(tks[0])){
				Integer v = mIdTypeStat1.get(tks[0]);
				mIdTypeStat1.put(tks[0], v + cat1.get(k));
			}else
				mIdTypeStat1.put(tks[0], cat1.get(k));
			if(!mHit.containsKey(tks[0]+"\t"+tks[1]+"\t"+tks[2])){
				mHit.put(tks[0]+"\t"+tks[1]+"\t"+tks[2], 1);
				if(mIdTypeStat2.containsKey(tks[0])){
					Integer v = mIdTypeStat2.get(tks[0]);
					mIdTypeStat2.put(tks[0], v + 1);
				}else
					mIdTypeStat2.put(tks[0], 1);
			}
			if(!mHit.containsKey(tks[0]+"\t"+tks[1])){
				mHit.put(tks[0]+"\t"+tks[1], 1);
				if(mId.containsKey(tks[0])){
					Integer v = mId.get(tks[0]);
					mId.put(tks[0], v + 1);
				}else
					mId.put(tks[0], 1);
			}
		}
		List<String> lstCheck = new ArrayList<String>();
		it = (Iterator) mId.keySet().iterator();
		while(it.hasNext()){
			String type = (String) it.next();
			String toadd = name + "\t" + type + "\t"+mIdTypeStat1.get(type)+ "\t"+mIdTypeStat2.get(type)+ "\t"+mId.get(type);
			lstCheck.add(toadd);
		}
		
		bubleSort(lstCheck, 4, 0, lstCheck.size());
		bubleSecondSortBy(lstCheck, 4, 3);
		bubleSecondSortBy(lstCheck, 3, 2);
		
		Map<String , String> mCats = new HashMap<String, String>();
		Map<String , String> mBlack = new HashMap<String, String>();
		boolean black = false;
		for(int i = 0 ; i<lstCheck.size(); i++){
//System.err.println("check:" + lstCheck.get(i));
			String [] tks = lstCheck.get(i).split("\t");
			if(tks.length!=5)continue;
			String v = mCats.get(tks[0]);
			if(v!=null){
				String[] tkstks = v.split("\t");
					for(int j = 0; j<tkstks.length; j++){
						if(mCK.containsKey(tkstks[j]+"\t"+tks[1])){
//							if(mBlack.containsKey(tks[0])){
//								mBlack.put(tks[0], mBlack.get(tks[0])+"\t"+tks[1]);
//							}else
//								mBlack.put(tks[0], tks[1]);
							mBlack.put(tks[1], "");
							black = true;
							break;
						}
					}
			}
			if(black == true)
				continue;
			if(mCats.containsKey(tks[0])){
				mCats.put(tks[0], mCats.get(tks[0])+"\t"+tks[1]);
			}else
				mCats.put(tks[0], tks[1]);
		}
		
		Map<String, Integer> mXX = new HashMap<String,Integer>(); 
		mXX.clear();
		for(int i = 0 ; i < inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length == 6){
				String [] tkstks = tks[3].split(":");
				tkstks = tkstks[0].split("_");
				if(tkstks.length > 1 && mBlack.containsKey(tkstks[tkstks.length-2])){
//System.err.println("black1:" + inKv.values.get(i));
					inKv.values.set(i, null);
					continue;
				}
				tkstks = tks[3].split(":");
				if(mXX.containsKey(tkstks[0])){
					mXX.put(tkstks[0], 1+ mXX.get(tkstks[0]));
				}else
					mXX.put(tkstks[0], 1);
			}
		}
		
		for(int i = 0 ; i < inKv.values.size(); i++){
			String val = inKv.values.get(i);
			if(val == null)continue;
			String [] tks = val.split("\t");
			if(tks.length == 6){
				String [] tkstks = tks[3].split(":");
				if(mXX.containsKey(tkstks[0]) && mXX.get(tkstks[0])>60000){
//System.err.println("black2:" + inKv.values.get(i));
					inKv.values.set(i, null);
				}
			}
		}
		
	}

}
