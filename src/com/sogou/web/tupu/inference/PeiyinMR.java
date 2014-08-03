package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class PeiyinMR extends MyMR {
	public Map<String, String> mTag = null;
	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/tag.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			mTag = new HashMap<String, String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 1)
					continue;
				mTag.put(seg[0], "");
			}
			br.close();
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}
	}
	
	private void addTagLine(String id, String url, String name, String tag, TaskInputOutputContext context ) throws IOException, InterruptedException{
		try {
			String md5str = InferenceMR.getMd5String(url+tag);
//MyMR.reduceOutput(null,"MD5:" + url+tag + ", " + md5str, context);
			MyMR.reduceOutput(id, url+"\t"+name+"\t标签:标签_标签名@" +md5str+"\t"+ tag + "\t-1", context);
			MyMR.reduceOutput(id, url+"\t"+name+"\t标签:标签_权重@" +md5str+"\t100\t-1", context);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException, InterruptedException {
		List<String> properties = inKv.values;
		Set<String> addedSet = new HashSet<String>();
		Set<String> originalSet = new HashSet<String>();
		boolean isCartoon = false;
		boolean isCartoonTV = false;
		boolean isGroup = false;
		boolean isMainLand = false;
		boolean hasWar = false;
		boolean isOld = false;
		boolean isJapan = false;
		boolean isHorrible = false;
		boolean isReligion = false;
		String url = null, name = null;
		for (String property_line : properties) {
			String[] tks = property_line.split("\t");
			if (tks.length == 6) {
				if(!isJapan){
					if(tks[3].equals("简介") || tks[3].contains("剧情")){
						if(tks[4].contains("抗日")
								||tks[4].contains("日本")
								||tks[4].contains("中日")){
							isJapan = true;
						}
					}
				}
				if (tks[3].equals("电影_电影类型")) {
					if (tks[4].equals("动画") || tks[4].equals("动漫")) {
						isCartoon = true;
					}
					if(mTag.containsKey(tks[4])){
						addedSet.add(tks[4]);
					}
					if(tks[4].contains("战争")){
						hasWar = true;
					}
					if(tks[4].contains("古装")
							||tks[4].contains("古代")){
						isOld = true;
					}
					if(url == null){
						url = tks[1];
					}
					if(name == null){
						name = tks[2];
					}
				} else if (tks[3].equals("电视剧_类型")) {
					if (tks[4].equals("动画") || tks[4].equals("动漫")) {
						isCartoonTV = true;
					}
					if(mTag.containsKey(tks[4])){
						addedSet.add(tks[4]);
					}
					if(tks[4].contains("战争")){
						hasWar = true;
					}
					if(tks[4].contains("古装")
							||tks[4].contains("古代")){
						isOld = true;
					}
					if(url == null){
						url = tks[1];
					}
					if(name == null){
						name = tks[2];
					}
				}
				
				if(tks[3].equals("电影_制片地区") || tks[3].equals("电视剧_制片地区")){
					if(url == null){
						url = tks[1];
					}
					if(name == null){
						name = tks[2];
					}
					if(tks[4].equals("中国大陆")){
						isMainLand = true;
					}
				}
				
				if(tks[3].startsWith("标签:标签_标签名")){
					if(tks[4].equals("恐怖")||tks[4].equals("惊悚")){
						isHorrible = true;
					}
					if(tks[4].startsWith("宗教")){
						isReligion = true;
					}
					originalSet.add(tks[4]);
				}
				if(tks[1].startsWith("Sogoucomp_")){
					isGroup = true;
				}
			}
		}
		Iterator it = (Iterator) addedSet.iterator();
		while(it.hasNext()){
			String tag = (String) it.next();
			if(!originalSet.contains(tag))
				addTagLine(inKv.key, url , name, tag, context );
		}
		if(url != null && name != null ){
			if(hasWar && isMainLand  && !isOld && isJapan && !addedSet.contains("抗日") && !originalSet.contains("抗日")){
				addTagLine(inKv.key, url, name, "抗日", context);
				addedSet.add("抗日");
			}
			if(isHorrible && name.startsWith("鬼") && !name.contains(":") && !addedSet.contains("鬼")&& !originalSet.contains("鬼")){
				addTagLine(inKv.key, url, name, "鬼", context);
				addedSet.add("鬼");
			}
			if(isReligion && (name.contains("佛") || name.contains("禅")) && !addedSet.contains("佛教")&& !originalSet.contains("佛教")){
				addTagLine(inKv.key, url, name, "佛教", context);
				addedSet.add("佛教");
			}
			if(name.contains("奥特曼")&& !addedSet.contains("奥特曼")&& !originalSet.contains("奥特曼")){
				addTagLine(inKv.key, url, name, "奥特曼", context);
				addedSet.add("奥特曼");
			}
			if(name.contains("高考")&& !addedSet.contains("高考")&& !originalSet.contains("高考")){
				addTagLine(inKv.key, url, name, "高考", context);
				addedSet.add("高考");
			}
			if(name.contains("篮球")&& !addedSet.contains("篮球")&& !originalSet.contains("篮球")){
				addTagLine(inKv.key, url, name, "篮球", context);
				addedSet.add("篮球");
			}
			if( ( name.contains("蟒") || name.contains("蛇") ) && isHorrible && !addedSet.contains("蟒蛇")&& !originalSet.contains("蟒蛇")){
				addTagLine(inKv.key, url, name, "蟒蛇", context);
				addedSet.add("蟒蛇");
			}
		}
		
		for (int i = 0; i < properties.size(); i++) {
			String property_line = properties.get(i);
			String[] tks = property_line.split("\t");
			if (tks.length == 6) {
				if (isCartoon) {
					if (tks[3].contains("电影_演员表:电影演员表属性_演员")) {
						property_line = property_line.replace(
								"电影_演员表:电影演员表属性_演员", "电影_演员表:电影配音属性_演员");
					}
					if (tks[3].equals("电影_主演")) {
						property_line = property_line.replace("电影_主演",
								"电影_配音演员");
					}
				}
				if (isCartoonTV) {
					if (tks[3].contains("电视剧_演员表:电视剧演员表属性_演员")) {
						property_line = property_line.replace(
								"电视剧_演员表:电视剧演员表属性_演员", "电视剧_演员表:电视剧配音属性_演员");
					}
					if (tks[3].equals("电视剧_主演")) {
						property_line = property_line.replace("电视剧_主演",
								"电视剧_配音演员");
					}
				}
			}
			if (isGroup) {
				property_line = property_line.replace("电影_演员表:电影演员表属性_演员",
						"电影系列_系列演员表:系列电影演员表_演员");
				property_line = property_line.replace("电影_演员表:电影演员表属性_角色",
						"电影系列_系列演员表:系列电影演员表_角色");
				property_line = property_line.replace("电影_演员表:电影演员表属性_权重",
						"电影系列_系列演员表:系列电影演员表_权重");

				property_line = property_line.replace("电视剧_演员表:电视剧演员表属性_演员",
						"电视剧系列_系列演员表:电视剧系列演员表_演员");
				property_line = property_line.replace("电视剧_演员表:电视剧演员表属性_角色",
						"电视剧系列_系列演员表:电视剧系列演员表_角色");
				property_line = property_line.replace("电视剧_演员表:电视剧演员表属性_权重",
						"电视剧系列_系列演员表:电视剧系列演员表_权重");
			}
			properties.set(i, property_line);
		}
		return;
	}

}
