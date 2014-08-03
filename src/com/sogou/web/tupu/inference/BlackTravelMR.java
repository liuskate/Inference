package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class BlackTravelMR extends MyMR {

	Set<String> mTravelBlack = null;
	Map<String, String> mTravelTable = null;

	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/lvyou.black";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mTravelBlack = new HashSet<String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 2)
					continue;
				mTravelBlack.add(seg[0] + "\t" + seg[1]);
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}

		filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/Lvyou.table";
		configPath = new Path(filePath);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mTravelTable = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 2)
					continue;
				mTravelTable.put(seg[0], seg[1]);
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
		for (int i = 0; i < outKv.values.size(); i++) {
			String val = outKv.values.get(i);
			String[] tks = val.split("\t");
			if (tks.length == 5 && tks[0].equals("REFERENCE")
					&& tks[2].contains("@IN")) {
				if (tks[2].contains("旅游景点_地域")) {
					if (!mTravelBlack.contains(tks[1] + "\t" + tks[3])
							&& !tks[1].equals(tks[3])) {
						MyMR.reduceOutput(outKv.key, val, context);
						outKv.values.set(i, null);
					}
				} else {
					MyMR.reduceOutput(outKv.key, val, context);
					outKv.values.set(i, null);

				}
			}

		}
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
			if (tks.length == 6 && tks[3].equals("旅游景点_地域")) {
				if (mTravelBlack.contains(tks[2] + "\t" + tks[4])) {
					inKv.values.set(i, null);
				}
			} else if (tks.length == 6 && tks[3].equals("旅游景点_主题")) {
				String sTheme = tks[4];
				if (mTravelTable.containsKey(tks[4])) {
					sTheme = mTravelTable.get(tks[4]);
					if (sTheme.equals("#删掉#")) {
						inKv.values.set(i, null);
					} else
						inKv.values.set(i, tks[0] + "\t" + tks[1] + "\t"
								+ tks[2] + "\t" + tks[3] + "\t" + sTheme + "\t"
								+ tks[5]);
				}
			}
		}

	}

}
