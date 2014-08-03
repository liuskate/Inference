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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class FoundInClickMR extends MyMR {

	Map<String, String> mSiteTitle = null;
	Map<String, String> mSiteMD5 = null;

	@Override
	public void myInitializerMapSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/click_site.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mSiteTitle = new HashMap<String, String>();
			mSiteMD5 = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 3)
					continue;
				mSiteTitle.put(seg[0], seg[1]);
				mSiteMD5.put(seg[0], seg[2]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
	}

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if (inKv.values.size() != 1)
			return;
		String val = inKv.values.get(0);
		String[] tks = val.split("\t");
		if (tks.length == 6 && tks[1].contains("http:")) {
			String[] tkstks = tks[1].split("/");
			if (tkstks.length > 3) {
				if (mSiteTitle.containsKey(tkstks[2])) {
					// url => I_FC_M, id, site, siteTitle, siteMD5;
					MyMR.mapOutput(tks[1],"I_FC_M\t" + tks[0] + "\t" + tkstks[2]
									+ "\t" + mSiteTitle.get(tkstks[2]) + "\t"
									+ mSiteMD5.get(tkstks[2]), context);
				}
			}
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if (!inKv.key.contains("http:"))
			return;
		String url = inKv.key;
		Integer count = null;
		for (int i = 0; i < inKv.values.size(); i++) {
			String value = inKv.values.get(i);
			String[] tks = value.split("\t");
			if (tks.length == 2) {
				count = Integer.parseInt(tks[1]);
			}
		}
		if (count == null)
			count = 0;
		for (int i = 0; i < inKv.values.size(); i++) {
			String value = inKv.values.get(i);
			String[] tks = value.split("\t");
			if (tks.length == 5 && tks[0].equals("I_FC_M")) {
				// id => I_FC_R, url, count, site, siteTitle, siteMD5;
				MyMR.reduceOutput(tks[1], "I_FC_R\t" + url + "\t" + count + "\t" + tks[2] + "\t"
								+ tks[3] + "\t" + tks[4], context);
			}
		}

	}

	class SiteInfo {
		String url;
		String siteTitle;
		String siteMD5;
		Integer count;
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		Map<String, SiteInfo> mSite = new HashMap<String, SiteInfo>();
		String name = null;
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val == null)continue;
			String[] tks = val.split("\t");
			if (name == null && tks.length == 6) {
				name = tks[2];
			}
			if (tks.length == 7 && val.contains("I_FC_R")) {
				Integer n = Integer.parseInt(tks[3]);
				if (mSite.containsKey(tks[4])) {
					SiteInfo si = mSite.get(tks[4]);
					if (n > si.count) {
						si.count = n;
						si.siteTitle = tks[5];
						si.siteMD5 = tks[6];
						si.url = tks[2];
					}
				} else {
					SiteInfo si = new SiteInfo();
					si.count = n;
					si.siteTitle = tks[5];
					si.siteMD5 = tks[6];
					si.url = tks[2];
					mSite.put(tks[4], si);
				}
				inKv.values.set(i, null);
			}
		}
		if (name == null)
			return;
		Iterator it = (Iterator) mSite.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Entry) it.next();
			String site = (String) entry.getKey();
			SiteInfo si = (SiteInfo) entry.getValue();
			MyMR.reduceOutput(inKv.key, "REFERENCE\t" + name
							+ "\t—”…Ï‘ƒ∂¡:—”…Ï‘ƒ∂¡ Ù–‘_Õ¯’æµÿ÷∑@IN" + si.siteMD5 + "\t"
							+ si.url + "\t-1", context);
			MyMR.reduceOutput(inKv.key, "REFERENCE\t" + name
							+ "\t—”…Ï‘ƒ∂¡:—”…Ï‘ƒ∂¡ Ù–‘_Õ¯’æ√˚@IN" + si.siteMD5 + "\t"
							+ si.siteTitle + "\t-1", context);
		}

	}

}
