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

public class GameSoftMR extends MyMR {

	@Override
	public void myInitializerMap(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		boolean propneed = false;
		if(inKv.values.size()==1){
			String val = inKv.values.get(0);
			String [] tks = val.split("\t");
			if(tks.length==7){
				if(tks[4].contains("Èí¼þ_") 
						|| tks[4].contains("ÓÎÏ·_")){
					MyMR.mapOutput(inKv.key, "GS", context);
				}
			}
		}
	}

	@Override
	public void myInitializerReduce(MyKeyValue inKv, MyKeyValue outKv,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		boolean isGS = false;
		for(int i = 0 ; i<inKv.values.size(); i++){
			String val = inKv.values.get(i);
			String [] tks = val.split("\t");
			if(tks.length==1 && tks[0].equals("GS")){
				isGS = true;
				inKv.values.set(i, null);
			}
		}
		if(isGS){
			for(int i = 0 ; i < inKv.values.size(); i++){
				MyMR.reduceOutput(null, inKv.values.get(i), context);
			}
		}
		
	}



}
