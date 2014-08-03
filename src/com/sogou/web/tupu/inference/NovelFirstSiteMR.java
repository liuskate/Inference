package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class NovelFirstSiteMR extends MyMR {

	Map<String, String> mNovelSite = null;

	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/novel.site";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mNovelSite = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 2)
					continue;
				mNovelSite.put(seg[0], seg[1]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!\n", filePath);
			System.exit(1);
		}
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		String url = "", name = null;
		Set<String> added = new HashSet<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val == null) continue;
			String[] tks = val.split("\t");
			if (tks.length == 6) {
				if (!added.contains(tks[1])) {
					if (tks[1].contains("http")
							&& !tks[1].contains("baike.baidu.com")
							&& !tks[1].contains("hudong.com")
							&& !tks[1].contains("http://www.17k.com/zuozhe")) {
						String[] tkstks = tks[1].split("/");
						if (tkstks.length > 0
								&& tkstks[tkstks.length - 1].length() != 32) {
							url = url + "@@" + tks[1];
						}
					}
					added.add(tks[1]);
					name = tks[2];
				}
			}
		}
		String[] tks = url.split("@@");
		if (tks.length == 2) {
			url = tks[1];
			tks = url.split("/");
			if (tks.length > 3) {
				if (mNovelSite.containsKey(tks[2])) {
					MyMR.reduceOutput(inKv.key, url + "\t" + name + "\t网络小说_首发网站\t"
							+ mNovelSite.get(tks[2]) + "\t-1", context);
				}
			}
		}

	}

}
