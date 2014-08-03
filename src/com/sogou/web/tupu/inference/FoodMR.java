package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class FoodMR extends MyMR {

	Map<String, String> mTagName = new HashMap<String, String>();

	@Override
	public void myInitializerMapSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/shicai.composition.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mTagName = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 2)
					continue;
				mTagName.put(seg[0], seg[1]);
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
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(!val.contains("食材_营养成分表:"))continue;
			String[] tks = val.split("\t");
			if (tks.length == 6 && tks[3].contains("食材_营养成分表:")) {
				String[] tkstks = tks[3].split("@");
				if (tkstks.length == 2) {
					if (tks[3].contains("营养成分属性_营养成分名称")) {
						if (mTagName.containsKey(tks[4]))
							MyMR.mapOutput("FD_" + tks[0] + "_"
									+ tkstks[1], "I_FD_M_T\t"
									+ tks[1] + "\t" + tks[2] + "\t" + tks[4]+"\t"+mTagName.get(tks[4]), context);
					}
					if (tks[3].contains("营养成分属性_权值"))
						MyMR.mapOutput("FD_" + tks[0] + "_" + tkstks[1], "I_FD_M_W\t" + tks[1] + "\t" + tks[2]
										+ "\t" + tks[4], context);
				}

			}
		}
	}


	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if (!inKv.key.startsWith("FD_"))
			return;
		String tag = null, tagname = null, weight = null, url = null, name = null, id = null, inid = null;
		String[] tks = inKv.key.split("_");
		if (tks.length != 3)
			return;
		id = tks[1];
		inid = tks[2];
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(!val.contains("I_FD_M"))continue;
			tks = val.split("\t");
			if(tks.length>= 4 && tks[0].startsWith("I_FD_M")){
				if (url == null) {
					url = tks[1];
					name = tks[2];
				}
				if (tks[0].equals("I_FD_M_T")) {
					tag = tks[3];
					tagname = tks[4];
				} else if (tks[0].equals("I_FD_M_W")) {
					weight = tks[3];
				}
			}
		}
		if (tag == null || weight == null)
			return;
		MyMR.reduceOutput(tag, "I_R_FD\t"
					+ id + "\t" + url + "\t" + name + "\t" + weight + "\t"
					+ tagname, context);
	}
	
	public static class TagCount{
		public TagCount(String str, float n){
			tagline = str;
			count = n ;
		}
		public String getTagLine(){
			return tagline;
		}
		public float getCount(){
			return count;
		}
		@Override
		public String toString() {
			return tagline.split("\t")[2] +":"+ count;
		}
		private String tagline;
		private float count;
	}
	
	
	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		int nTotal = 0;
		List<String> tagList = new ArrayList<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			if (val.contains("I_R_FD")) {
				nTotal++;
			}
		}
		int topN = 0;
		if(nTotal * 0.1 > 20){
			topN = (int) (nTotal * 0.1);
		}else{
			topN = 20;
		}
		Queue<TagCount> queue = new PriorityQueue<TagCount>(topN,
				new Comparator<TagCount>() {
					@Override
					public int compare(TagCount o1, TagCount o2) {
						return (int) (o1.getCount() - o2.getCount());
					}
				});
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			if (val.contains("I_R_FD")) {
				String[] tks = val.split("\t");
				if (tks.length == 7) {
					float count = Float.parseFloat(tks[5]);
					if(tks[5].contains("e")){
						count = Float.parseFloat(tks[5].substring(0, tks[5].indexOf("e")));
					}
					if(queue.size()==topN){
						TagCount tc = queue.peek();
						if( tc.getCount() < count ){
							queue.poll();
							queue.add(new TagCount(val, count));
						}
					}else{
						queue.add(new TagCount(val, count));
					}
				}
				inKv.values.set(i, null);
			}
		}
		int n = queue.size();
		while (queue.size()>0) {
			String[] tks = ((TagCount) queue.poll()).getTagLine().split("\t");
			int inid = n + tks[6].charAt(0);
			if (tks.length == 7) {
				MyMR.reduceOutput(tks[2], tks[3] + "\t" + tks[4]
								+ "\t食材_营养成分标签:营养成分标签属性_名称@" + inid
								+ "\t" + tks[6] + "\t-1", context);
				MyMR.reduceOutput(tks[2], tks[3] + "\t" + tks[4]
								+ "\t食材_营养成分标签:营养成分标签属性_权值@" + inid
								+ "\t" + n + "\t-1", context);
			}
			n--;
		}
	}
	
	public static void printQueue(Queue queue){
		Iterator it = queue.iterator(); 
		while(it.hasNext()){
			System.out.printf("%s, ", it.next().toString());
		}
		System.out.printf("\n");
	}

	public static void main(String [] argv){
		
		System.out.println(Float.MAX_VALUE);
		Queue<TagCount> queue = new PriorityQueue<TagCount>(5,
				new Comparator<TagCount>() {
					@Override
					public int compare(TagCount o1, TagCount o2) {
						return (int) (o1.getCount() - o2.getCount());
					}
				});
		float [] a = {3, (float) 5.1, (float) 9.0, (float) 2.1, (float) 3.1, (float) 4.9, (float) 9.8, (float) 6.4, (float) 23.1, (float) 1.9};
		for(int i = 0; i<10; i++){
			if(queue.size()==5){
				TagCount tc = queue.peek();
				if( tc.getCount() < a[i] ){
					queue.poll();
					queue.add(new TagCount(""+i, a[i]) );
				}
			}else{
				queue.add(new TagCount(String.valueOf(i), a[i]));
			}
			printQueue(queue);
		}
	}
}
