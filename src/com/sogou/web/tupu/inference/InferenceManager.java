package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class InferenceManager {
	private String phase = null;
	private List<MyMR> mrlist = null;
	private List<String> mrNameList = null;
	
	public InferenceManager(Configuration conf) throws Exception{
		phase = MyMR.getConfigureValue(conf,"inference.phase.name");
		mrlist = new ArrayList<MyMR>();
		String mrClassesName = null;
		//job1
		if(phase.startsWith("Init")){
			mrClassesName = MyMR.getConfigureValue(conf,"initializer.classes");
		}
		//job2
		else if(phase.startsWith("Inference")){
			mrClassesName = MyMR.getConfigureValue(conf,"inference.classes");
		}
		//job3
		else if(phase.startsWith("ReservedProcess")){
				mrClassesName = MyMR.getConfigureValue(conf,"reservedprocess.classes");
		}
		//job4
		else if(phase.startsWith("ReservedInference")){
				mrClassesName = MyMR.getConfigureValue(conf,"reservedinference.classes");
		}
		//job3
		else if(phase.startsWith("MessUp")){
			mrClassesName = MyMR.getConfigureValue(conf,"messup.classes");
		}
		//job4
		else if(phase.startsWith("MessClean")){
			mrClassesName = MyMR.getConfigureValue(conf,"messclean.classes");
		}
		mrlist = new ArrayList<MyMR>();
		mrNameList = new ArrayList<String>();
		
		String [] classnames = mrClassesName.split(",");
		for(String classname : classnames){
			if(classname==null || classname.length() == 0)continue;
			mrNameList.add(classname);
			MyMR mr = (MyMR) Class.forName("com.sogou.web.tupu.inference."+classname).newInstance();
System.out.println("InferenceManager: "+ phase +" "+ classname);
			if(phase.equals("InitMapper")){
				mr.myInitializerMapSetup(conf);
			}else if(phase.equals("InitReducer")){
				mr.myInitializerReduceSetup(conf);
			}else if(phase.equals("InferenceMapper")){
				mr.myInferenceMapSetup(conf);
			}else if(phase.equals("InferenceReducer")){
				mr.myInferenceReduceSetup(conf);
			}else if(phase.equals("ReservedProcessMapper")){
				mr.myInitializerMapSetup(conf);
			}else if(phase.equals("ReservedProcessReducer")){
				mr.myInitializerReduceSetup(conf);
			}else if(phase.equals("ReservedInferenceMapper")){
				mr.myInferenceMapSetup(conf);
			}else if(phase.equals("ReservedInferenceReducer")){
				mr.myInferenceReduceSetup(conf);
			}else if(phase.equals("MessUpMapper")){
				mr.myInitializerMapSetup(conf);
			}else if(phase.equals("MessUpReducer")){
				mr.myInitializerReduceSetup(conf);
			}else if(phase.equals("MessCleanMapper")){
				mr.myInferenceMapSetup(conf);
			}else if(phase.equals("MessCleanReducer")){
				mr.myInferenceReduceSetup(conf);
			}
			mrlist.add(mr);
		}
	}
	
	//inKv:ÐÞ¸Ä,É¾³ý
	//outKv:ÐÂÔö
	public void process(MyKeyValue inKv, MyKeyValue outKv, TaskInputOutputContext context) throws IOException, InterruptedException{
		for(int i = 0; i<mrlist.size(); i++){
			if(phase.startsWith("InitMapper")){
				mrlist.get(i).myInitializerMap(inKv, outKv, context);
			}else if(phase.startsWith("InitReducer")){
				mrlist.get(i).myInitializerReduce(inKv, outKv, context);
			}else if(phase.startsWith("InferenceMapper")){
				mrlist.get(i).myInferenceMap(inKv, outKv, context);
			}else if(phase.startsWith("InferenceReducer")){
				mrlist.get(i).myInferenceReduce(inKv, outKv, context);
			}else if(phase.equals("ReservedProcessMapper")){
				mrlist.get(i).myInitializerMap(inKv, outKv, context);
			}else if(phase.equals("ReservedProcessReducer")){
				mrlist.get(i).myInitializerReduce(inKv, outKv, context);
			}else if(phase.equals("ReservedInferenceMapper")){
				mrlist.get(i).myInferenceMap(inKv, outKv, context);
			}else if(phase.equals("ReservedInferenceReducer")){
				mrlist.get(i).myInferenceReduce(inKv, outKv, context);
			}else if(phase.startsWith("MessUpMapper")){
				mrlist.get(i).myInitializerMap(inKv, outKv, context);
			}else if(phase.startsWith("MessUpReducer")){
				mrlist.get(i).myInitializerReduce(inKv, outKv, context);
			}else if(phase.startsWith("MessCleanMapper")){
				mrlist.get(i).myInferenceMap(inKv, outKv, context);
			}else if(phase.startsWith("MessCleanReducer")){
				mrlist.get(i).myInferenceReduce(inKv, outKv, context);
			}
		}
	}
}
