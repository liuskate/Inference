package com.sogou.web.tupu.inference;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MessUp extends Configured implements Tool{
	public static class MessUpMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			line = line.trim();
			String[] tks = line.split("\t");
			if(tks.length<2)return;
			MyKeyValue inKv = new MyKeyValue(tks[0], line);
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
			conf.set("inference.phase.name", "MessUpMapper");
			try {
				im = new InferenceManager(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		InferenceManager im = null;
	}
	
	public static class MessUpReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			MyKeyValue inKv = new MyKeyValue(key.toString(), null);
			for(Text v : values){
				inKv.values.add(v.toString());
			}
			
			MyKeyValue outKv = new MyKeyValue();
			im.process(inKv, outKv, context);
			/*
			for(String v : inKv.values){
				MyMR.reduceOutput(inKv.key, v, context);
			}
			*/
			for(String v : outKv.values){
					MyMR.reduceOutput(outKv.key, v, context);
			}
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf.set("inference.phase.name", "MessUpReducer");
			try {
				im = new InferenceManager(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		InferenceManager im = null;
	}
	
	@SuppressWarnings("deprecation")
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
			
			inputPath = cmd.getOptionValue("i", null);
			outputPath = cmd.getOptionValue("o", null);
		}catch(Exception e){
			HelpFormatter helpformat = new HelpFormatter();
			helpformat.printHelp("Initializer", opts);
			e.printStackTrace();
			System.exit(1);
		}
		Configuration conf = this.getConf();
		Job job = new Job(conf, "Messup");
		job.setJarByClass(MessUp.class);
		job.setMapperClass(MessUpMapper.class);
		job.setReducerClass(MessUpReducer.class);
		job.setNumReduceTasks(conf.getInt("messup.mapreduce.num.reduce", 200));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		if(inputPath == null){
			inputPath = MyMR.getConfigureValue(conf,"fs.path.messup.inputs");
			FileInputFormat.addInputPaths(job, inputPath);
		}else
			FileInputFormat.addInputPath(job, new Path(inputPath));
		
		if(outputPath == null){
			outputPath = MyMR.getConfigureValue(conf,"fs.path.messup.output");
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
	
	public static void main(String[] args) throws Exception{
		Tool tool = new MessUp();
		Configuration conf = HBaseConfiguration.create();
		Main.getConf(conf, args);
		System.out.println("messup configuration:" + MyMR.getConfigureValue(conf,"messup.classes"));
		ToolRunner.run(conf, tool,args);
	}

}
