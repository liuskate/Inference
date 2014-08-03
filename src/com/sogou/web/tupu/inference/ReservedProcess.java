package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ReservedProcess extends Configured implements Tool{
	
	public static class ReservedProcessMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			line = line.trim();
			//if(!line.startsWith("10580178"))return;
			String[] tks = line.split("\t");
			if(tks.length!=6)return;
			MyKeyValue inKv = new MyKeyValue(tks[0], null);
			LongWritable pos = (LongWritable) key;
			inKv.values.add("ORDER"+pos.toString() +"\t"+line);
			MyKeyValue outKv = new MyKeyValue(); 

			im.process(inKv, outKv, context);
			for(String v : inKv.values){
				MyMR.mapOutput(inKv.key, v, context);
			}
			for(String v : outKv.values){
				MyMR.mapOutput(outKv.key, v, context);
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set("inference.phase.name", "ReservedProcessMapper");
			try {
				im = new InferenceManager(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		InferenceManager im = null;
	}
	
	public static class ReservedProcessReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			MyKeyValue inKv = new MyKeyValue(key.toString(), null);
			List<String> oriList = new ArrayList<String>();
			List<String> otherList = new ArrayList<String>();
			for(Text v : values){
				String str = v.toString();
				if(str.startsWith("ORDER")){
					oriList.add(str.substring(5));
				}else
					otherList.add(str);
			}
			
			Collections.sort(oriList, new Comparator() {
				public int compare(Object o1, Object o2) {
					String s1 = ((String)o1);
					String s2 = ((String)o2);
					long n1 = Long.parseLong(s1.substring(0,s1.indexOf("\t")));
					long n2 = Long.parseLong(s2.substring(0,s2.indexOf("\t")));
					if(n1-n2<0){
						return -1;
					}else if(n1-n2==0){
						return 0;
					}else
						return 1;
				}
			});
			
			for(int i = 0; i<oriList.size(); i++){
				String val = oriList.get(i);
				int start = val.indexOf("\t");
				oriList.set(i, val.substring(start+1));
			}
			inKv.values.addAll(oriList);
			inKv.values.addAll(otherList);
			
			MyKeyValue outKv = new MyKeyValue();
			im.process(inKv, outKv, context);
//			for(String v : inKv.values){
//				MyMR.reduceOutput(null, v, context);
//			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set("inference.phase.name", "ReservedProcessReducer");
			try {
				im = new InferenceManager(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		InferenceManager im = null;
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption(OptionBuilder.hasArg(true).isRequired(false)
				.withDescription("Input Path").withLongOpt("input").create("i"));
		opts.addOption(OptionBuilder.hasArg(true).isRequired(false)
				.withDescription("Output Path").withLongOpt("output").create("o"));
		opts.addOption(OptionBuilder.hasArg(true).isRequired(false)
				.withDescription("Cluster name").withLongOpt("cluster").create("c"));
		
		String inputPath = null;
		String outputPath = null;
		
		PosixParser parser = null;
		CommandLine cmd = null;
		try{
			parser = new PosixParser();
			cmd = parser.parse(opts, args);
			
			inputPath = cmd.getOptionValue("i");
			outputPath = cmd.getOptionValue("o");
		}catch(Exception e){
			HelpFormatter helpformat = new HelpFormatter();
			helpformat.printHelp("Inference", opts);
			e.printStackTrace();
			System.exit(1);
		}
		Configuration conf = this.getConf();
		Job job = new Job(conf, "ReservedProcess");
		job.setJarByClass(ReservedProcess.class);
		job.setMapperClass(ReservedProcessMapper.class);
		job.setReducerClass(ReservedProcessReducer.class);
		job.setNumReduceTasks(100);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		if(inputPath == null){
			inputPath = MyMR.getConfigureValue(conf,"fs.path.reservedprocess.inputs");
			FileInputFormat.addInputPaths(job, inputPath);
		}else
			FileInputFormat.addInputPath(job, new Path(inputPath));
		
		if(outputPath == null){
			outputPath = MyMR.getConfigureValue(conf,"fs.path.reservedprocess.output");
		}
		Path path = new Path(outputPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(path)){
			fs.delete(path);
		}
		FileOutputFormat.setOutputPath(job, path);
		job.waitForCompletion(true);
		return 0;
	}
	
	
	@SuppressWarnings("null")
	public static void main(String[] args) throws Exception{
		Tool tool = new ReservedProcess();
		Configuration conf = HBaseConfiguration.create();
		Main.getConf(conf, args);
		System.out.println("Reserved Process configuration:" + MyMR.getConfigureValue(conf,"reservedprocess.classes"));
		ToolRunner.run(conf, tool, args);
	}
}
