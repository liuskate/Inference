package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class BlackMR extends MyMR {

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			String[] tks = val.split("\t");
			if (tks.length == 6 && tks[3].equals("汉字_偏旁")) {
				MyMR.mapOutput(tks[1], "I_M_ZC\t" + tks[0], context);
			} else if (tks.length == 6 && tks[3].equals("汉字_笔画")) {
				MyMR.mapOutput(tks[1], "I_M_ZC", context);
			}
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		boolean yes = false;
		String name = null;
		List<String> ids = new ArrayList<String>();
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
			if (val.startsWith("I_M_ZC")) {
				if (tks.length == 2) {
					ids.add(tks[1]);
				} else if (tks.length == 1) {
					yes = true;
				}
			}
			if (tks.length == 6 && tks[3].startsWith("食材_")) {
				name = tks[2];
			}
		}
		if (ids.size() > 0 && yes == true) {
			for (int i = 0; i < ids.size(); i++)
				MyMR.reduceOutput(ids.get(i), "I_R_ZCPP\t" + inKv.key, context);
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
		}else{
			System.err.printf("load file %s err!", filePath);
			System.exit(1);
		}

	}

	public String processFoodXianghaValue(String val) {
		val = val
				.replaceAll("^\"|\"$", "")
				.replaceAll(" ?~ ?", "~")
				.replaceAll(" ?\\( ?", "(")
				.replaceAll(" ?\\) ?", ")")
				.replaceAll(" ?- ?| ?？ ?| ?？ ?", "-")
				.replaceAll(" ?“ ?", "“")
				.replaceAll(" ?” ?", "”")
				.replaceAll("、 ", "、")
				.replaceAll(" 。", "。")
				.replaceAll(" ；", "；")
				.replaceAll(" 痛", "痛")
				.replaceAll("一等 ", "一等")
				.replaceAll("二等 ", "二等")
				.replaceAll("统货 ", "统货")
				.replaceAll(" ?r- ?", "r-")
				.replaceAll("防癌抗癌 美容", "防癌抗癌、美容")
				.replaceAll(" 一级 二级 三级 四级 五级 六级 细紧有锋苗 紧细尚有锋苗 尚紧实 尚紧 稍松 粗松 ",
						"一级、二级、三级、四级、五级、六级、细紧有锋苗、紧细尚有锋苗、尚紧实、尚紧、稍松、粗松")
				.replaceAll(" 润肺化痰 治疗阴虚久咳 痨嗽痰血 ", "润肺化痰、治疗阴虚久咳、痨嗽痰血")
				.replaceAll(" B1 ", "B1").replaceAll(" B2 ", "B2")
				.replaceAll(" ?B ?", "B").replaceAll(" 倍", "倍")
				.replaceAll("的 ", "的").replaceAll("另 含", "另含")
				.replaceAll("有 强", "有强").replaceAll("2 蕨菜", "2.蕨菜");
		if (val.matches("[0-9]\\. ")) {
			val = val.replaceAll("\\. ", ".");
		}
		return val;
	}

	private String processFoodMeishijValue(String val) {
		val = val.replaceAll("1\\.? ", " 1.").replaceAll("2\\.? ", " 2.")
				.replaceAll("3\\.? ", " 3.").replaceAll("4\\.? ", " 4.")
				.replaceAll("5\\.? ", " 5.").replaceAll("6\\.? ", " 6.")
				.replaceAll("7\\.? ", " 7.").replaceAll("8\\.? ", " 8.")
				.replaceAll("9\\.? ", " 9.").replaceAll("10\\.? ", " 10.")
				.replaceAll("^\"|^\\.|\"$", "").replaceAll(" ?~ ?", "~")
				.replaceAll(" ?， ?", "，").replaceAll(" ?\\( ?", "(")
				.replaceAll(" ?\\) ?", ")").replaceAll(" ?- ?", "-")
				.replaceAll(" ?— ?", "-").replaceAll(" ?“ ?", "")
				.replaceAll("、 ", "、").replaceAll("。 ", "。")
				.replaceAll(" ；", "；").replaceAll(" ?r- ?", "r-")
				.replaceAll("另外 ", "另外");
		return val;
	}

	public String processIdiomCharactor(String[] tks) {
		String charactor = null;
		String s1 = "", s2 = "",s3 = "", s4 = "";
		if(tks[2].length()>0)
			s1 = tks[2].substring(0, 1);
		if(tks[2].length()>1)
			s2 = tks[2].substring(1, 2);
		if(tks[2].length()>2)
			s3 = tks[2].substring(2, 3);
		if(tks[2].length()>3)
			s4 = tks[2].substring(3, 4);
		int a1 = 1, a2, a3, a4, bs1 = a1, bs3;
		if (s2.equals(s1)) {
			a2 = bs1;
		} else {
			a2 = a1 + 1;
		}
		int bs2 = a2;
		if (s3.equals(s1)) {
			a3 = bs1;
		} else if (s3.equals(s2)) {
			a3 = bs2;
		} else {
			a3 = a2 + 1;
		}
		bs3 = a3;
		if (s4.equals(s1)) {
			a4 = bs1;
		} else if (s4.equals(s2)) {
			a4 = bs2;
		} else if (s4.equals(s3)) {
			a4 = bs3;
		} else
			a4 = a3 + 1;
		String val = tks[4];
		val = val.replaceAll("A", "1").replaceAll("B", "2")
				.replaceAll("C", "3").replaceAll("C", "3");
		String str = a1 + "" + a2 + "" + a3 + "" + a4;
		if (tks[2].equals("假惺惺")) {
			charactor = "ABB";
		} else if (tks[2].equals("团团转")) {
			charactor = "AAB";
		} else if (tks[4].equals("ABAB")) {
			if (s2.equals(s4)) {
				charactor = tks[4];
			} else
				charactor = "ABAC";
		} else if (str.equals(val)) {
			charactor = tks[4];
		} else {
			charactor = str.replaceAll("1", "A").replaceAll("2", "B")
					.replaceAll("3", "C");
		}
		return charactor;
	}

	@Override
	public void myInferenceReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		Set<String> urls = new HashSet<String>();
		boolean isFood = false, isXiangha = false, isMmbang = false, isMeishij = false, isBoohee = false, is911cha = false;
		boolean isFilter = false;
		int size = inKv.values.size();
		for (int i = 0; i < size; i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
			if (tks.length == 3 && tks[1].equals("I_R_ZCPP")) {
				urls.add(tks[2]);
				inKv.values.set(i, null);
			}

			if (tks.length == 6) {
				if (tks[3].contains("食材_"))
					isFood = true;
				if (tks[1].contains("www.xiangha.com"))
					isXiangha = true;
				if (tks[1].contains("www.mmbang.com"))
					isMmbang = true;
				if (tks[1].contains("www.meishij.net"))
					isMeishij = true;
				if (tks[1].contains("www.boohee.com"))
					isBoohee = true;
				if (tks[1].contains("yingyang.911cha.com"))
					is911cha = true;

				if (!blackInference(tks[2], tks[3], tks[4])) {
					if (tks[1].contains("www.xiangha.com")) {
						if (tks[3].equals("食材_功效")
								|| tks[3].equals("食材_营养价值")
								|| tks[3].equals("食材_选购")
								|| tks[3].equals("食材_存储")
								|| tks[3].equals("食材_烹饪技巧")) {
							if (!tks[4].contains("，") && !tks[4].contains("。")
									&& !tks[4].contains("、")
									&& !tks[4].contains("；")
									&& !tks[4].contains("：")) {
								continue;
							}
							tks[4] = processFoodXianghaValue(tks[4]);
							inKv.values.set(i, null);
							String[] tkstks = tks[4].split(" ");
							if (tkstks.length == 1) {
								inKv.values.set(i, tks[0]+"\t"+tks[1] + "\t" + tks[2]
										+ "\t" + tks[3] + "\t" + tks[4] + "\t"
										+ tks[5]);
							} else {
								String tmpVal = tkstks[0];
								for (int j = 1; j < tkstks.length; j++) {
									if (!tkstks[j].equals("")) {
										if (tkstks[j]
												.matches("^[a-zA-Z]+|[a-zA-Z]$")) {
											tmpVal = tmpVal + " " + tkstks[j];
										} else {
											inKv.values.add(tks[0]+"\t"+tks[1] + "\t"
													+ tks[2] + "\t" + tks[3]
													+ "\t" + tmpVal + "\t"
													+ tks[5]);
											tmpVal = tkstks[j];
										}
										if (j == tkstks.length - 1) {
											inKv.values.add(tks[0]+"\t"+tks[1] + "\t"
													+ tks[2] + "\t" + tks[3]
													+ "\t" + tmpVal + "\t"
													+ tks[5]);
										}
									}
								}
							}
						}
					} else if (tks[1].contains("www.meishij.net")) {
						if (tks[3].equals("食材_功效")
								|| tks[3].equals("食材_营养价值")
								|| tks[3].equals("食材_选购")
								|| tks[3].equals("食材_存储")
								|| tks[3].equals("食材_烹饪技巧")) {
							if (!tks[4].contains("，") && !tks[4].contains("。")
									&& !tks[4].contains("、")
									&& !tks[4].contains("；")
									&& !tks[4].contains("："))
								continue;
							if ((tks[4].startsWith("^宜") || tks[4]
									.startsWith("忌")) && tks[4].contains("+")) {
								inKv.values.set(i, null);
								continue;
							}
							if (tks[3].equals("食材_营养价值")
									|| tks[3].equals("食材_选购")
									|| tks[3].equals("食材_存储")
									|| tks[3].equals("食材_烹饪技巧"))
								continue;
							tks[4] = processFoodMeishijValue(tks[4]);
							String[] tkstks = tks[4].split(" ");
							inKv.values.set(i, null);
							if (tkstks.length == 1) {
								inKv.values.set(i, tks[0] + "\t" + tks[1] + "\t" + tks[2]
										+ "\t" + tks[3] + "\t" + tks[4] + "\t"
										+ tks[5]);
							} else {
								String tmpVal = tkstks[0];
								for (int j = 1; j < tkstks.length; j++) {
									if (!tkstks[j].equals("")) {
										if (tkstks[j]
												.matches("^[a-zA-Z]+|[a-zA-Z]$")) {
											tmpVal = tmpVal + " " + tkstks[j];
										} else {
											if (tmpVal.length() != 1)
												inKv.values.add(tks[0] +"\t"+ tks[1] + "\t"
														+ tks[2] + "\t"
														+ tks[3] + "\t"
														+ tmpVal + "\t"
														+ tks[5]);
											tmpVal = tkstks[j];
										}
										if (j == tkstks.length - 1
												&& tmpVal.length() != 1) {
											inKv.values.add(tks[0] +"\t"+tks[1] + "\t"
													+ tks[2] + "\t" + tks[3]
													+ "\t" + tmpVal + "\t"
													+ tks[5]);
										}
									}
								}
							}
						}
					}
				} else {
					inKv.values.set(i, null);
				}
			}

		}
		if (isFood && !isXiangha && !isMmbang && !isMeishij && !isBoohee
				&& !is911cha) {
			isFilter = true;
		}

		int pianpangIndex = -1;
		String pianpang = null;
		String firstPianpang = null;
		boolean addedTongyi = false;
		for (int i = 0; i < inKv.values.size(); i++) {
			String val = inKv.values.get(i);
			if(val==null)continue;
			String[] tks = val.split("\t");
			if (tks.length == 6) {
				if (tks[3].equals("汉字_偏旁")) {
					if (urls.contains(tks[1])) {
						pianpang = val;
					} else if (firstPianpang == null) {
						firstPianpang = val;
					}
					inKv.values.set(i, null);
					pianpangIndex = i;
				}

				if (isFilter) {
					if (tks[3].contains("食材_"))
						inKv.values.set(i, null);
				}

				if (tks[3].equals("成语_特征") && tks[4].startsWith("A")) {
					String charactor = processIdiomCharactor(tks);
					if (charactor != null) {
						inKv.values.set(i, tks[0] +"\t"+ tks[1] + "\t" + tks[2] + "\t"
								+ tks[3] + "\t" + charactor + "\t" + tks[5]);
					}
				}

				if (tks[3].equals("同义词")) {
					if (!addedTongyi) {
						inKv.values.set(i, tks[0] +"\t"+ tks[1] + "\t" + tks[2] + "\t同义词\t"
								+ tks[2] + "\t" + tks[5]);
						addedTongyi = true;
					} else{
						inKv.values.set(i, null);
					}
				}

			}
		}

		if (pianpang == null && firstPianpang != null) {
			pianpang = firstPianpang;
		}
		if (pianpang != null){
			inKv.values.set(pianpangIndex, pianpang);
		}
	}

	public static void main(String[] args) {
		BlackMR bmr = new BlackMR();
		String line = "10504894	http://chengyu.itlearner.com/cy21/21875.html	万万千千	成语_特征	AABB	-1";
		String [] tks = line.split("\t");
		String x = bmr.processIdiomCharactor(tks);
		x = "123";
		String s1 = x.substring(0,1);
		String s2 = x.substring(0,5);
		String s3 = x.substring(4,5);
		System.err.println(x);
	}
}
