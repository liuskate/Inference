package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class InferenceMR extends MyMR {

	HashMap<String, HashSet<String>> propertyInference = null;
	HashSet<String> inferProperty = null;
	HashMap<String, HashSet<String>> mLinkProperty = null;

	@Override
	public void myInitializerMapSetup(Configuration conf) throws IOException {
		propertyInference = new HashMap<String, HashSet<String>>();
		inferProperty = new HashSet<String>();
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/Inference.conf";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 3)
					continue;
				HashSet<String> tmpProperty = propertyInference.get(seg[0]);

				if (!inferProperty.contains(seg[0]))
					inferProperty.add(seg[0]);
				String[] terms = seg[2].split("@");
				for (String term : terms) {
					if (!inferProperty.contains(term))
						inferProperty.add(term);
				}

				if (tmpProperty == null) {
					tmpProperty = new HashSet<String>();
					tmpProperty.add(line);
					propertyInference.put(seg[0], tmpProperty);
				} else {
					tmpProperty.add(line);
				}
			}

			if (!inferProperty.contains("别名"))
				inferProperty.add("别名");

			br.close();
		} else {
			System.err.println("load " + filePath + "err!");
			System.exit(1);
		}
	}

	// find links need to inference
	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		if (inKv.values.size() != 1)
			return;
		String line = inKv.values.get(0);
		String[] seg = line.split("\t");
		if (seg.length != 6)
			return;
		String sourceId = seg[5];
		if (sourceId.equals("-1") || sourceId.equals("NULL"))
			return;
		String property = property(seg[3]);
		// for urlPV
		if (this.inferProperty.contains(property)) {
			MyMR.mapOutput(seg[0], "I_IF_M\t" + sourceId, context);
		}
	}

	@Override
	public void myInitializerReduceSetup(Configuration conf) throws IOException {
		myInitializerMapSetup(conf);
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		HashSet<String> sourceIds = new HashSet<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String value = inKv.values.get(i);
			if (value.startsWith("I_IF_M")) {
				String[] tks = value.split("\t");
				if (tks.length != 2)
					continue;
				if (sourceIds.contains(tks[1]))
					continue;
				sourceIds.add(tks[1]);
			}
		}
		for (String sourceId : sourceIds) {
			// MyKeyValue myKv = new MyKeyValue(sourceId, null);
			for (int i = 0; i < inKv.values.size(); i++) {
				String value = inKv.values.get(i);
				String[] tks = value.split("\t");
				if (value.startsWith(inKv.key) && tks.length == 6) {
					// myKv.values.add("I_IF_R\t" + value);
					String property = tks[3];
					property = property(property);
					if (!inferProperty.contains(property))
						continue;
					MyMR.reduceOutput(sourceId, "I_IF_R\t" + value, context);
				}
			}
		}
	}

	HashSet<String> sBlackInferenceZW = null;
	HashSet<String> sBlackInferenceWB = null;

	boolean blackInference(String name, String property, String val) {
		String[] tks = property.split("@");
		String[] tkstks = tks[0].split(":");
		if (sBlackInferenceZW.contains(name + "\t" + tkstks[0])
				|| sBlackInferenceWB.contains(tkstks[0] + "\t" + val))
			return true;
		return false;
	}

	@Override
	public void myInferenceReduceSetup(Configuration conf) throws IOException {
		myInitializerMapSetup(conf);
		String filePath = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/blackinference";
		Path configPath = new Path(filePath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(configPath) && fs.isFile(configPath)) {
			sBlackInferenceZW = new HashSet<String>();
			sBlackInferenceWB = new HashSet<String>();
			FSDataInputStream in = fs.open(configPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in,
					"GBK"));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				String[] seg = line.split("\t");
				if (seg.length != 3)
					continue;
				if (seg[0].equals("ZW")) {
					sBlackInferenceZW.add(seg[1] + "\t" + seg[2]);
				} else if (seg[0].equals("WB")) {
					sBlackInferenceWB.add(seg[1] + "\t" + seg[2]);
				}
			}
			br.close();
		} else {
			System.err.println("load " + filePath + "err!");
			System.exit(1);
		}
	}
	
	private void processCartoonActor(MyKeyValue inKv){
		List<String> properties = inKv.values;
		Map<String, Boolean> mIsCartoon = new HashMap<String, Boolean>();
		Map<String, Boolean> mIsCartoonTV = new HashMap<String, Boolean>();
		for (String property_line : properties) {
			String[] tks = property_line.split("\t");
			if (tks.length == 8 && property_line.contains("I_IF_R")) {
				if (tks[5].equals("电影_电影类型")) {
					if (tks[6].equals("动画") || tks[6].equals("动漫")) {
						mIsCartoon.put(tks[2], true);
					}
				} else if (tks[5].equals("电视剧_类型")) {
					if (tks[6].equals("动画") || tks[6].equals("动漫")) {
						mIsCartoonTV.put(tks[2], true);
					}
				}
			}
		}

		for (int i = 0; i < properties.size(); i++) {
			String property_line = properties.get(i);
			String[] tks = property_line.split("\t");
			if (tks.length == 8 && property_line.contains("I_IF_R")) {
				if (mIsCartoon.containsKey(tks[2])) {
					if (tks[5].contains("电影_演员表:电影演员表属性_演员")) {
						property_line = property_line.replace(
								"电影_演员表:电影演员表属性_演员", "电影_演员表:电影配音属性_演员");
					}
					if (tks[5].equals("电影_主演")) {
						property_line = property_line.replace("电影_主演",
								"电影_配音演员");
					}
				}
				if (mIsCartoonTV.containsKey(tks[2])) {
					if (tks[5].contains("电视剧_演员表:电视剧演员表属性_演员")) {
						property_line = property_line.replace(
								"电视剧_演员表:电视剧演员表属性_演员", "电视剧_演员表:电视剧配音属性_演员");
					}
					if (tks[5].equals("电视剧_主演")) {
						property_line = property_line.replace("电视剧_主演",
								"电视剧_配音演员");
					}
				}
			}
			properties.set(i, property_line);
		}	
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) {
		
		processCartoonActor(inKv);
		
		String sourceId = inKv.key;
		String iToName = null;
		int nTotal = 0;
		HashMap<String, HashSet<PropertyValue>> urlPV = new HashMap<String, HashSet<PropertyValue>>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val== null)continue;
			// System.err.println("inKv.key = " + inKv.key + ", inKv.values = "
			// + val);
			String[] tks = val.split("\t");
			String property = null;
			if (tks.length == 8 && val.contains("I_IF_R")) {
				nTotal++;
				// inKv.values.set(i, null);
				property = tks[5];
				property = property(property);
				if (!inferProperty.contains(property))
					continue;
				HashSet<PropertyValue> info = urlPV.get(tks[2]);
				if (info == null) {
					info = new HashSet<PropertyValue>();
					PropertyValue pv = new PropertyValue();
					pv.Property = tks[5];
					pv.Value = tks[6];
					pv.Source = tks[7];
					info.add(pv);
					urlPV.put(tks[2], info);
				} else {
					PropertyValue newInfo = new PropertyValue();
					newInfo.Property = tks[5];
					newInfo.Value = tks[6];
					newInfo.Source = tks[7];
					info.add(newInfo);
				}

			} else if (tks.length == 6 && iToName == null) {
				property = tks[3];
				property = property(property);
				if (!inferProperty.contains(property))
					continue;
				iToName = tks[2];
			}
		}
		HashSet<String> addedSet = new HashSet<String>();
		String tmpProperty, value, uri, property, addedKey;
		String toUrl = "REFERENCE";
		for (int i = 0; iToName != null && nTotal > 0 && i < inKv.values.size(); i++) {
			String line = inKv.values.get(i);
			if(line == null)continue;
			String[] seg = line.split("\t");
			if (seg.length != 8 || !line.contains("I_IF_R"))
				continue;
			if (seg[3].startsWith("manualprize_"))
				continue;
			tmpProperty = seg[5];
			value = seg[6];
			// sourceId=seg[6];

			if (sourceId.equals(seg[7])) {// only inference value having link
				property = property(tmpProperty);
				// System.out.println(property);
				if (propertyInference.containsKey(property)) {
					uri = null;
					try {
						uri = getMd5String(seg[2]);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					String oriUri = uri;
					String toValue = value.trim();
					// System.out.println(seg[1]);
					// String uri=tools.getMd5String(seg[0]);
					HashSet<String> hsInfer = propertyInference.get(property);
					Iterator<String> it = hsInfer.iterator();
					// String iToName=inneridName.get(sourceId);
					while (it.hasNext()) {
						String inferLine = (String) it.next();
						// System.out.println(inferLine);
						String[] terms = inferLine.split("\t");
						String toProperty = terms[1];
						String judgeProperty = terms[2];
						// System.out.println(judgeProperty);
						if (!judgeProperty.contains("@")) {
							HashSet<PropertyValue> hsPV = urlPV.get(seg[2]);
							Iterator<PropertyValue> itPV = hsPV.iterator();
							// System.out.println(judgeProperty);
							uri = oriUri;
							while (itPV.hasNext()) {
								PropertyValue pv = (PropertyValue) itPV.next();
								String iProperty = pv.Property;
								String iFromId = pv.Source;
								iProperty = property(iProperty);
								// System.out.println(judgeProperty+"\t"+iProperty);
								if (judgeProperty.equals("名称")) {
									if (!blackInference(iToName, toProperty,
											seg[4])) {
										String val = toUrl + "\t" + iToName
												+ "\t" + toProperty + "@IN"
												+ uri + "\t" + seg[4] + "\t"
												+ seg[2];
										addedKey = seg[2]+toProperty + uri + seg[4];
										if (!addedSet.contains(addedKey)) {
											outKv.key = sourceId;
											outKv.values.add(val);
											addedSet.add(addedKey);
										}
									}
									break;
								}
								// System.out.println(judgeProperty+"\t"+iProperty);
								if (judgeProperty.equals(iProperty)) {
									if (!blackInference(iToName, toProperty,
											pv.Value)) {
										String val = toUrl + "\t" + iToName
												+ "\t" + toProperty + "@IN"
												+ uri + "\t" + pv.Value + "\t"
												+ iFromId;
										addedKey = iFromId+toProperty + uri + pv.Value;
										if (!addedSet.contains(addedKey)) {
											outKv.key = sourceId;
											outKv.values.add(val);
											addedSet.add(addedKey);
										}
									}
								}
							}
						} else {
							String judgeFirst = judgeProperty.split("@")[0];
							boolean blankNode = false;
							if (judgeFirst.startsWith("#")) {
								blankNode = true;
								judgeFirst = judgeFirst.substring(1);
							}
							String judgeSecondOri = judgeProperty.split("@")[1];
							String judgeSecond = judgeSecondOri;
							HashSet<PropertyValue> hsPV = urlPV.get(seg[2]);
							List<PropertyValue> lpv = getListPV(hsPV,
									judgeFirst, toValue);
							for (PropertyValue pv : lpv) {
								if (pv != null) {
									Iterator<PropertyValue> itPV = hsPV
											.iterator();
									// System.out.println(pv.Property);
									judgeSecond = judgeSecondOri;
									String judgeName = judgeSecond;
									if (pv.Property.contains("@")) {
										judgeSecond = judgeSecond + "@"
												+ pv.Property.split("@")[1];
										if (blankNode)
											try {
												uri = getMd5String(oriUri
														+ pv.Property
																.split("@")[1]);
											} catch (NoSuchAlgorithmException e) {
												e.printStackTrace();
											}
										else
											uri = oriUri;
									}

									// System.out.println(judgeSecond);
									while (itPV.hasNext()) {
										PropertyValue ipv = (PropertyValue) itPV
												.next();
										String iProperty = ipv.Property;
										String iFormId = ipv.Source;
										if (iProperty.contains(":")) {
											String[] iterms = iProperty
													.split(":");
											iProperty = iterms[1];
										}
										// System.out.println(iProperty);
										if (judgeName.equals("名称")) {
											if (!blackInference(iToName,
													toProperty, seg[4])) {
												String val = toUrl + "\t"
														+ iToName + "\t"
														+ toProperty + "@IN"
														+ uri + "\t" + seg[4]
														+ "\t" + seg[2];
												addedKey = seg[2]+toProperty + uri
														+ seg[4];
												if (!addedSet
														.contains(addedKey)) {
													outKv.key = sourceId;
													outKv.values.add(val);
													addedSet.add(addedKey);
												}
											}
											break;
										}

										if (judgeSecond.equals(iProperty)
												|| judgeName.equals(iProperty)) {
											if (!blackInference(iToName,
													toProperty, ipv.Value)) {
												String val = toUrl + "\t"
														+ iToName + "\t"
														+ toProperty + "@IN"
														+ uri + "\t"
														+ ipv.Value + "\t"
														+ iFormId;
												addedKey = iFormId+toProperty + uri
														+ ipv.Value;
												if (!addedSet
														.contains(addedKey)) {
													outKv.key = sourceId;
													outKv.values.add(val);
													addedSet.add(addedKey);
												}
											}
										}

									}
								}
							}
						}
					}
				}
			}
		}

		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if (val.contains("I_IF_R\t"))
				inKv.values.set(i, null);
		}
	}

	public static String getMd5String(String key)
			throws NoSuchAlgorithmException {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'a', 'b', 'c', 'd', 'e', 'f' };
		java.security.MessageDigest mdInstance = java.security.MessageDigest
				.getInstance("MD5");
		byte[] md;
		char str[] = null;
		try {
			md = mdInstance.digest(key.getBytes("GBK"));
			int j = md.length;
			str = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new String(str);
	}

	public static String property(String propertyToken) {
		if (propertyToken.contains(":")) {
			String[] terms = propertyToken.split(":");
			propertyToken = terms[1].split("@")[0];
		} else
			propertyToken = propertyToken.split("@")[0];
		return propertyToken.trim();
	}

	public static List<PropertyValue> getListPV(HashSet<PropertyValue> hsPV,
			String judgeP, String value) {
		List<PropertyValue> listPV = new ArrayList<PropertyValue>();
		PropertyValue pv = null;
		Iterator<PropertyValue> it = hsPV.iterator();
		while (it.hasNext()) {
			PropertyValue tmpPV = (PropertyValue) it.next();

			if (tmpPV.Property.contains(judgeP) && tmpPV.Value.contains(value)) {
				// System.out.println(tmpPV.Property+"\t"
				// +judgeP+"\t"+tmpPV.Value+"\t"+value);
				listPV.add(tmpPV);
			}
		}
		return listPV;

	}

	public static class PropertyValue {
		public String Property = new String();
		public String Value = new String();
		public String Source = new String();
		public String Url = new String();

		public PropertyValue() {
			this.Url = new String();
			this.Property = new String();
			this.Value = new String();
			this.Source = new String();
		}

		public int hashCode() {
			return Property.hashCode() + Value.hashCode() * 10;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof PropertyValue) {
				PropertyValue p = (PropertyValue) obj;
				if (this.Property.equals(p.Property) && this.Value == p.Value) {
					return true;
				} else {
					return false;
				}
			}
			return false;
		}
	}
}
