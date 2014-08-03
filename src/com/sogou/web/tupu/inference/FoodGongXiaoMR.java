package com.sogou.web.tupu.inference;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;


public class FoodGongXiaoMR extends MyMR {

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
//		if(inKv.values.size()==1){
//			String val = inKv.values.get(0);
//			String [] tks = val.split("\t");
//			if(tks.length==7){
//					if(tks[4].contains("景点")){
//						outKv.key = inKv.key;
//						outKv.values.add(val);
//					}
//			}
//		}
	}

	public static class MD5 {
		public static String str;

		public static String getMD5(String plainText) {
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(plainText.getBytes());
				byte b[] = md.digest();
				int i;
				StringBuffer buf = new StringBuffer("");
				for (int offset = 0; offset < b.length; offset++) {
					i = b[offset];
					if (i < 0)
						i += 256;
					if (i < 16)
						buf.append("0");
					buf.append(Integer.toHexString(i));
				}
				str = buf.toString();
				return str;
				// System.out.println("result: " + buf.toString());// 32位的加密
				// System.out.println("result: " + buf.toString().substring(8,
				// 24));// 16位的加密
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return plainText;
			}
		}
	}

	public String getMd5(String id, int count) {
		String md5 = id + "####" + count;
		return MD5.getMD5(md5);
	}

	public static Pattern startPattern = Pattern.compile("^[0-9]+\\.");
	public static Pattern NumPattern = Pattern.compile("[0-9]+\\.");
	public static Pattern yystartPattern = Pattern.compile("^[0-9]+\\、");
	public static Pattern yyNumPattern = Pattern.compile("[0-9]+\\、");
	public static Pattern reliangPattern = Pattern.compile("([0-9]+)[^克]");
	public String split = "[0-9]+\\.";
	public String yysplit = "[0-9]+\\、";
	public static String or6 = "3";

	public static String tmpFile;
	public static String oldValue = "食材_功效";
	public static String title = "食材_功效表:功效表属性_标题";
	public static String content = "食材_功效表:功效表属性_内容";
	public static String quan = "食材_功效表:功效表属性_权重";
	public static String reliang = "食材_热量";

	public static String yyoldValue = "食材_营养价值";
	public static String yytitle = "食材_营养价值表:营养价值表属性_标题";
	public static String yycontent = "食材_营养价值表:营养价值表属性_内容";
	public static String yyquan = "食材_营养价值表:营养价值表属性_权重";

	public void processFood(TaskInputOutputContext context,
			List<Integer> allGongxiao, MyKeyValue inKv, String oldValue,
			String title, String quan, String content, Pattern startPattern,
			Pattern NumPattern, String split) throws UnsupportedEncodingException, IOException, InterruptedException {

		String id = null, link = null, name = null, property = null, value = null, add = null;
		String mapid = inKv.key;
		int blankid = 100000;
		int quanValue = 100000;
		String preTitle = "";
		boolean startFlag = true;
		for (int i = 0; i < allGongxiao.size(); i++) {
			String[] arr = inKv.values.get(allGongxiao.get(i)).split("\t");
			boolean added = false;
			id = arr[0];
			link = arr[1];
			name = arr[2];
			add = arr[5];
			value = arr[4];
//System.err.println("num of tks = " + arr.length);
//System.err.println("line:" + inKv.values.get(allGongxiao.get(i)));
			Matcher matcher = startPattern.matcher(value);
			Matcher yyMatcher = yystartPattern.matcher(value);
			if (matcher.find()) {
				String preValue = value;
				int pos = value.indexOf(".");
				value = value.substring(pos + 1);
				startFlag = false;
				String[] splitArr = value.split(split);
				if (splitArr.length > 1) {
					for (String p : splitArr) {
						String blankmd5 = getMd5(mapid, blankid);
						property = title + "@" + blankmd5;
						MyMR.reduceOutput(id, link + "\t" + name + "\t" + property + "\t"
										+ p + "\t" + add, context);
						MyMR.reduceOutput(id,link + "\t" + name + "\t" + quan
										+ "@" + blankmd5 + "\t" + quanValue
										+ "\t" + add, context);
						quanValue -= 100;
						blankid++;
					}
				} else {
					preTitle = value;
					String contentValue = "";
					if (i + 1 < allGongxiao.size()) {
						arr = inKv.values.get(allGongxiao.get(i+1)).split("\t");
						matcher = startPattern.matcher(arr[4]);
						if (!matcher.find()) {
							contentValue = arr[4];
							i++;
						}
					}
					String blankmd5 = getMd5(mapid, blankid);
					property = title + "@" + blankmd5;
					MyMR.reduceOutput(id, link
							+ "\t" + name + "\t" + property + "\t" + preTitle
							+ "\t" + add, context);
					MyMR.reduceOutput(id,link
							+ "\t" + name + "\t" + quan + "@" + blankmd5 + "\t"
							+ quanValue + "\t" + add, context);
					if (!contentValue.isEmpty()) {
						MyMR.reduceOutput(id,link + "\t" + name + "\t" + content
										+ "@" + blankmd5 + "\t" + contentValue
										+ "\t" + add, context);
					}
					blankid++;
					quanValue -= 100;
				}
				
				//add
				splitArr = preValue.split(split);
				if (splitArr.length > 1) {
					added = true;
					Matcher mm = NumPattern.matcher(preValue);
					for (int j = 0; j < splitArr.length; j++) {
//System.err.println("matcher seg:"+splitArr[j]);
						if (splitArr[j].isEmpty())
							continue;
						if (mm.find()) {
							String writeValue = mm.group() + splitArr[j].trim();
//System.err.println("matcher write:"+writeValue);
							MyMR.reduceOutput(id,link + "\t" + name + "\t"
									+ oldValue + "\t" + writeValue + "\t" + add, context);
						}
					}
				}
				
			} else if (yyMatcher.find()) {
				String preValue = value;
				int pos = value.indexOf("、");
				value = value.substring(pos + 1);
				startFlag = false;
				String[] splitArr = value.split(yysplit);
				if (splitArr.length > 1) {
					for (String p : splitArr) {
						String blankmd5 = getMd5(mapid, blankid);
						property = title + "@" + blankmd5;
						MyMR.reduceOutput(id,link + "\t" + name + "\t" + property + "\t"
										+ p + "\t" + add, context);
						MyMR.reduceOutput(id, link + "\t" + name + "\t" + quan
										+ "@" + blankmd5 + "\t" + quanValue
										+ "\t" + add, context);
						quanValue -= 100;
						blankid++;
					}
				} else {
					preTitle = value;
					String contentValue = "";
					if (i + 1 < allGongxiao.size()) {
						arr = inKv.values.get(allGongxiao.get(i+1)).split("\t");
						matcher = yystartPattern.matcher(arr[4]);
						if (!matcher.find()) {
							contentValue = arr[4];
							i++;
						}
					}
					String blankmd5 = getMd5(mapid, blankid);
					property = title + "@" + blankmd5;
					MyMR.reduceOutput(id, link
							+ "\t" + name + "\t" + property + "\t" + preTitle
							+ "\t" + add, context);
							MyMR.reduceOutput(id, link
							+ "\t" + name + "\t" + quan + "@" + blankmd5 + "\t"
							+ quanValue + "\t" + add, context);
					if (!contentValue.isEmpty()) {
						MyMR.reduceOutput(id, link + "\t" + name + "\t" + content
										+ "@" + blankmd5 + "\t" + contentValue
										+ "\t" + add, context);
					}
					blankid++;
					quanValue -= 100;
				}
				
				//add
				splitArr = preValue.split(yysplit);
				if (splitArr.length > 1) {
					added = true;
					Matcher mm = NumPattern.matcher(preValue);
					for (int j = 0; j < splitArr.length; j++) {
//System.err.println("yymatcher seg:"+splitArr[j]);
						if (splitArr[j].isEmpty())
							continue;
						if (mm.find()) {
							String writeValue = mm.group() + splitArr[j].trim();
//System.err.println("yymatcher write:"+writeValue);
							MyMR.reduceOutput(id, link + "\t" + name + "\t"
									+ oldValue + "\t" + writeValue + "\t" + add, context);
						}
					}
				}
				
			} else if (startFlag) {
				String blankmd5 = getMd5(mapid, blankid);
				property = title + "@" + blankmd5;
				MyMR.reduceOutput(id, link + "\t" + name + "\t" + property + "\t" + value
								+ "\t" + add, context);
								MyMR.reduceOutput(id, link
						+ "\t" + name + "\t" + quan + "@" + blankmd5 + "\t"
						+ quanValue + "\t" + add, context);
				quanValue -= 100;
				blankid++;
			}
			if(!added){
				MyMR.reduceOutput(null, inKv.values.get(allGongxiao.get(i)), context);
			}else
				inKv.values.set(allGongxiao.get(i), null);
		}
		allGongxiao.clear();
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		List<Integer> lstGongXiao = new ArrayList<Integer>();
		List<Integer> lstYY = new ArrayList<Integer>();
		Set<String> addedSet = new HashSet<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
//System.err.println("line: " + tks.length +"\t"+val);
			if (tks.length == 6 && tks[3].equals("食材_功效")) {
				if(!addedSet.contains(tks[4])){
					lstGongXiao.add(i);
					addedSet.add(tks[4]);
				}else{
					inKv.values.set(i, null);
				}
			} else if (tks.length == 6 && tks[3].equals("食材_营养价值")) {
				if(!addedSet.contains(tks[4])){
					lstYY.add(i);
					addedSet.add(tks[4]);
				}else{
					inKv.values.set(i, null);
				}
			} else if(tks.length==6 && tks[3].equals("食材_热量")) {
				if(!addedSet.contains(tks[4])){
					Matcher matcher = reliangPattern.matcher(tks[4]);
					if (matcher.find() && matcher.groupCount() == 1) {
						String num = matcher.group(1);
						MyMR.reduceOutput(tks[0], tks[1] + "\t" + tks[2] + "\t"
								+ "食材_标准热量" + "\t" + num + " 卡/100克" + "\t"
								+ tks[5], context);
					}
					addedSet.add(tks[4]);
				}else{
					inKv.values.set(i, null);
				}
			}
		}
		addedSet.clear();
//System.err.println(inKv.key + "\tlistGongXiao.size=" + lstGongXiao.size());
//System.err.println(inKv.key + "\tlstYY.size=" + lstYY.size());
		processFood(context, lstGongXiao, inKv, oldValue, title, quan, content, startPattern,
				NumPattern, split);
		lstGongXiao.clear();
		processFood(context,lstYY, inKv, yyoldValue, yytitle, yyquan, yycontent, startPattern,
				NumPattern, split);
		lstYY.clear();

	}

}
