package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class DateDynMR extends MyMR {

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			String [] tks = val.split("\t");
			if(tks.length==6){
				if(tks[3].contains("人物_国籍")
						||tks[3].contains("人物_出生日期")
						||tks[3].contains("已逝人物_逝世日期")
						||tks[3].equals("简介")){
					MyMR.reduceOutput(null, val, context);
				}
			}
		}
	}



}
