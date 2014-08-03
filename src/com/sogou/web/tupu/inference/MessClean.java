package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MessClean extends Configured implements Tool{
	
	public static class MessCleanMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			line = line.trim();
			//if(!line.startsWith("10580178"))return;
			String[] tks = line.split("\t");
			if(tks.length<2)return;
			MyKeyValue inKv = new MyKeyValue(tks[0], null);
			if(tks.length == 6){
				LongWritable pos = (LongWritable) key;
				inKv.values.add("ORDER"+pos.toString() +"\t"+line);
			}else
				inKv.values.add(line);
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
			conf.set("inference.phase.name", "MessCleanMapper");
			try {
				im = new InferenceManager(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		InferenceManager im = null;
	}
	
	public static class MessCleanReducer extends Reducer<Text, Text, Text, Text>{

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
					int n1 = Integer.parseInt(s1.substring(0,s1.indexOf("\t")));
					int n2 = Integer.parseInt(s2.substring(0,s2.indexOf("\t")));
					return n1-n2;
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
			for(String v : inKv.values){
				MyMR.reduceOutput(null, v, context);
			}
			for(String v : outKv.values){
				MyMR.reduceOutput(outKv.key, v, context);
			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set("inference.phase.name", "MessCleanReducer");
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
			helpformat.printHelp("MessClean", opts);
			e.printStackTrace();
			System.exit(1);
		}
		Configuration conf = this.getConf();
		Job job = new Job(conf, "MessClean");
		job.setJarByClass(MessClean.class);
		job.setMapperClass(MessCleanMapper.class);
		job.setReducerClass(MessCleanReducer.class);
		job.setNumReduceTasks(conf.getInt("messclean.mapreduce.num.reduce", 200));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		if(inputPath == null){
			inputPath = MyMR.getConfigureValue(conf,"fs.path.messclean.inputs");
			FileInputFormat.addInputPaths(job, inputPath);
		}else
			FileInputFormat.addInputPath(job, new Path(inputPath));
		
		if(outputPath == null){
			outputPath = MyMR.getConfigureValue(conf,"fs.path.messclean.output");
		}
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		String dateStr = df.format(new Date());
		outputPath = outputPath + dateStr;
		conf.set("fs.path.messclean.output", outputPath);
		Path path = new Path(outputPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(path)){
			System.err.println("to delete " + outputPath);
			fs.delete(path);
		}
		FileOutputFormat.setOutputPath(job, path);
		job.waitForCompletion(true);
		return 0;
	}
	
	@SuppressWarnings("null")
	public static void main(String[] args) throws Exception{
		Tool tool = new MessClean();
		Configuration conf = HBaseConfiguration.create();
		Main.getConf(conf, args);
		System.out.println("messclean configuration:" + MyMR.getConfigureValue(conf,"messclean.classes"));
		ToolRunner.run(conf, tool, args);
	}
}
