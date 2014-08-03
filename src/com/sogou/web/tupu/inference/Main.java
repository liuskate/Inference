package com.sogou.web.tupu.inference;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.fs.FsShell;

public class Main extends Configured implements Tool{
	public void copyInitRank(Configuration conf) throws IOException{
		String src = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/init.rank";
		Path rankPath = new Path(src);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(rankPath) && fs.isFile(rankPath)){
			String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/init.rank";
			System.out.println("copy " + src + " to " + dest);
			FSDataOutputStream ofs = fs.create(new Path(dest));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ofs, "GBK"));
			FSDataInputStream in = fs.open(rankPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] tks = line.split("\t");
				if(tks.length==2)
					out.write(tks[0]+"\tPAGERANK_IMPORTANCE\t"+tks[1]+"\n");
			}
			out.flush();
			out.close();
			br.close();
		}
	}

	public void copyAddWord(Configuration conf) throws IOException{
		String src = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/addword";
		Path rankPath = new Path(src);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(rankPath) && fs.isFile(rankPath)){
			String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/addword";
			System.out.println("copy " + src + " to " + dest);
			FSDataOutputStream ofs = fs.create(new Path(dest));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ofs, "GBK"));
			FSDataInputStream in = fs.open(rankPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				String[] tks = line.split("\t");
				if(tks.length==6)
					out.write(tks[0]+"\t"+tks[3]+"\t"+tks[4]+"\n");
			}
			out.flush();
			out.close();
			br.close();
		}
	}
	
	public void copyBlackpv(Configuration conf) throws IOException{
		String src = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/blackpv";
		Path rankPath = new Path(src);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(rankPath) && fs.isFile(rankPath)){
			String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/blackpv";
			System.out.println("copy " + src + " to " + dest);
			FSDataOutputStream ofs = fs.create(new Path(dest));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ofs, "GBK"));
			FSDataInputStream in = fs.open(rankPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				line=line.trim();
				out.write(line+"\tBLACK_PV\n");
			}
			out.flush();
			out.close();
			br.close();
		}
	}
	
	public void copyWordpv(Configuration conf) throws IOException{
		String src = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/wordpv";
		Path rankPath = new Path(src);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(rankPath) && fs.isFile(rankPath)){
			String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/wordpv";
			System.out.println("copy " + src + " to " + dest);
			FSDataOutputStream ofs = fs.create(new Path(dest));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ofs, "GBK"));
			FSDataInputStream in = fs.open(rankPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				String[] tks = line.trim().split("\t");
				if(tks.length==6)
					out.write(tks[0]+"\tSET_PV\t"+tks[4]+"\n");
			}
			out.flush();
			out.close();
			br.close();
		}
	}
	
	public void copyWeightpv(Configuration conf) throws IOException{
		String src = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/weight.conf";
		Path rankPath = new Path(src);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(rankPath) && fs.isFile(rankPath)){
			String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/weight.conf";
			System.out.println("copy " + src + " to " + dest);
			FSDataOutputStream ofs = fs.create(new Path(dest));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(ofs, "GBK"));
			FSDataInputStream in = fs.open(rankPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			String line;
			while ((line = br.readLine()) != null)
			{
				String[] tks = line.trim().split("\t");
				if(tks.length==2)
					out.write(tks[0]+"\tWEIGHT_PV\t"+tks[1]+"\n");
			}
			out.flush();
			out.close();
			br.close();
		}
	}
	
	public void copyVideoRank(Configuration conf) throws Exception{
		String src = "hdfs://cloud1016128162.wd.sjs.ss.nop.sogou-op.org/user/web_research/pdm/tupu/rank/video/video.rank.w.type*";
		Path rankPath = new Path(src);
		FileSystem srcFs = rankPath.getFileSystem( new Configuration() );
		FileStatus[] fstat = srcFs.globStatus(rankPath);
		System.err.println("file length="+fstat[fstat.length-1].getLen());
		if(fstat.length>0 && fstat[fstat.length-1].getLen() > 15858806
				&& fstat[fstat.length-1].getLen() < 30858806 ){
			Path rankFilePath = fstat[fstat.length-1].getPath();
			String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/video.rank.w.type";
			Path destPath = new Path(dest);
			FileSystem destFs = FileSystem.get(conf);
			if(destFs.exists(destPath)&&destFs.isFile(destPath)){
				System.out.println("copy " + dest + " to " + dest+".bak");
				FileUtil.copy(destFs, destPath, destFs, new Path(dest+".bak"), false, conf);
			}
			System.out.println("copy " + rankFilePath.toString() + " to " + dest);
			FileUtil.copy(srcFs, rankFilePath, destFs, new Path(dest), false, conf);
		}
	}
	
	public void copyQueryPv(Configuration conf) throws Exception{
		String src = "hdfs://cloud1016128162.wd.sjs.ss.nop.sogou-op.org/user/web_research/pdm/tupu/rank/hot/query.pv";
		Path pvPath = new Path(src);
		FileSystem srcFs = pvPath.getFileSystem( new Configuration() );
		
		FileSystem destFs = FileSystem.get(conf);
		String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/query.pv";
		Path destPath = new Path(dest + ".tmp");
		if(destFs.exists(destPath)){
			destFs.delete(destPath);
		}
		FileUtil.copyMerge(srcFs, pvPath, destFs, destPath, false, conf, null);
		
		FileStatus[] fstat = destFs.globStatus(destPath);
		System.err.println("file length="+fstat[fstat.length-1].getLen());
		if(fstat.length >=1 && fstat[fstat.length-1].getLen() > 158588060L && fstat[fstat.length-1].getLen() < 3585880600L ){
			System.err.println("update query.pv ");
			FileUtil.copy(destFs, destPath, destFs, new Path(dest), false, conf);
		}
		
	}
	
	
	public void copyMergeToLocal(String srcf, String dst, Configuration conf) throws IOException {
			System.out.println("copy " + srcf + " to " + dst);    
			Path srcPath = new Path(srcf);
		    FileSystem srcFs = srcPath.getFileSystem(conf);
		    Path [] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), 
		                                       srcPath);
		    FileSystem localfs = FileSystem.getLocal(conf);
		    if(localfs.exists(new Path(dst))){
		    	localfs.delete(new Path(dst));
		    }
		    for(int i=0; i<srcs.length; i++) {
		        FileUtil.copyMerge(srcFs, srcs[i], 
		                           FileSystem.getLocal(conf), new Path(dst), false, conf, null);
		    }
		  }   
	  
	public void copyFromLocal(Configuration conf) throws IOException{
		String src = MyMR.getConfigureValue(conf,"local.path.inference.data.unify");
		String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Merger/Output/data.unify";
		FileSystem fs = FileSystem.get(conf);
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/";
		src = MyMR.getConfigureValue(conf,"local.path.inference.merger.Lvyou.table");
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Merger/tmp/alias";
		src = MyMR.getConfigureValue(conf,"local.path.merger.tmp.alias");
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		FileSystem localfs = FileSystem.getLocal(conf);
		src = MyMR.getConfigureValue(conf,"local.path.inference.conf");
		Path srcConfPath = new Path(src);
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/conf/";
		Path dstConfPath = new Path(dest);
		if(localfs.isDirectory(srcConfPath)){
			FileStatus [] status = localfs.listStatus(srcConfPath);
			for(FileStatus stat : status){
				if(!stat.isDir()){
					System.out.println("copy " + stat.getPath().toString() + " to " + dest);
					fs.copyFromLocalFile(stat.getPath(), dstConfPath);
				}
			}
		}
		
		src = MyMR.getConfigureValue(conf,"local.path.inference.supplement");
		srcConfPath = new Path(src);
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/";
		dstConfPath = new Path(dest);
		if(localfs.isDirectory(srcConfPath)){
			FileStatus [] status = localfs.listStatus(srcConfPath);
			for(FileStatus stat : status){
				if(!stat.isDir()){
					System.out.println("copy " + stat.getPath().toString() + " to " + dest);
					fs.copyFromLocalFile(stat.getPath(), dstConfPath);
				}
			}
		}
		
	}

	public void copyReservedOutput(Configuration conf) throws IOException{
		String src = "data/lvyou.add";
		String dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/";
		FileSystem fs = FileSystem.get(conf);
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		src = "tmp/wordpv.black.manual.add";
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/";
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		
		src = "data/types.ranklist";
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/";
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		src = "data/people.dyn";
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/";
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		
		src = "data/game_soft_output";
		dest = MyMR.getConfigureValue(conf,"fs.path.inference.home")+"/Inference/supplement/";
		System.out.println("copy " + src + " to " + dest);
		fs.copyFromLocalFile(new Path(src), new Path(dest));

	}
	
	public boolean exeShell(String[] shellCommands) throws IOException{  
	    Runtime rt = Runtime.getRuntime();  
	    for(int i = 0; i<shellCommands.length; i++){
	    	BufferedReader br = null;
		    try {  
System.err.println(shellCommands[i]);
		        Process p = rt.exec(shellCommands[i]);
		        br = new BufferedReader(new InputStreamReader(p.getErrorStream()), 1024);
		        if(p.waitFor() != 0)  
		            return false;  
		    } catch (IOException e) {  
		        System.err.println("没有找到检测脚本");  
		        return false;  
		    } catch (InterruptedException e) {  
		        e.printStackTrace();  
		        return false;  
		    }
		    String line = null;
		    while(br!=null && (line = br.readLine()) != null ){
		    	System.err.println("LOG:"+line);
		    }
	    }
	    return true; 
	} 
	
	public final String[] shellCommands = {"sh -x bin/build_lvyou.sh tmp/data.unify.reservedprocess data/lvyou.add",
			"sh -x bin/build_rank_list.sh tmp/data.unify.reservedprocess data/novel.firstsite data/wordpv.black.manual data/types.ranklist",
			"sh -x bin/build_date_dyn.sh tmp/data.unify.reservedprocess data/people.dyn",
			"sh -x bin/build_game_soft.sh tmp/data.unify.reservedprocess data/game_soft_output"
	};
	
	
	public void reservedprocess(Configuration conf) throws IOException{
		copyMergeToLocal(conf.get("fs.path.reservedprocess.output"), "./tmp/data.unify.reservedprocess", conf);
		copyMergeToLocal(conf.get("fs.path.reservedinference.output"), "./tmp/data.unify.reservedinference", conf);
		
		String src = "./tmp/data.unify.reservedinference";
		Path rankPath = new Path(src);
		FileSystem localfs = FileSystem.getLocal(conf);
		if(localfs.exists(rankPath) && localfs.isFile(rankPath)){
			FSDataInputStream in = localfs.open(rankPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
			
			String destInference = "./data/inference.data.sort";
			System.out.println("copy " + src + " to " + destInference);
			FSDataOutputStream ofsInference = localfs.create(new Path(destInference));
			BufferedWriter outInference = new BufferedWriter(new OutputStreamWriter(ofsInference, "GBK"));
			
			String destRank = "./data/data.rank";
			System.out.println("copy " + src + " to " + destRank);
			FSDataOutputStream ofsRank = localfs.create(new Path(destRank));
			BufferedWriter outRank = new BufferedWriter(new OutputStreamWriter(ofsRank, "GBK"));
			
			String destWordpv = "./data/wordpv.black.manual";
			System.out.println("copy " + src + " to " + destWordpv);
			FSDataOutputStream ofsWordpv = localfs.create(new Path(destWordpv));
			BufferedWriter outWordpv = new BufferedWriter(new OutputStreamWriter(ofsWordpv, "GBK"));
			
			String destFirstSite = "./data/novel.firstsite";
			System.out.println("copy " + src + " to " + destFirstSite);
			FSDataOutputStream ofsFirstSite = localfs.create(new Path(destFirstSite));
			BufferedWriter outFirstSite = new BufferedWriter(new OutputStreamWriter(ofsFirstSite, "GBK"));
			
			String line;
			while ((line = br.readLine()) != null)
			{
				if(line.contains("旅游景点_地域")){
					outInference.write(line+"\n");
				}else if(line.contains("重要性")){
					outRank.write(line+"\n");
				}else if(line.contains("查询热度\t")){
					outWordpv.write(line+"\n");
				}else if(line.contains("网络小说_首发网站")){
					outFirstSite.write(line+"\n");
				}
			}
			outInference.flush();
			outInference.close();
			
			outRank.flush();
			outRank.close();
			
			outWordpv.flush();
			outWordpv.close();
			
			outFirstSite.flush();
			outFirstSite.close();
			
			br.close();
		}
		

	}
	
	public static void getConf(Configuration conf , String[] args){
		Options opts = new Options();
		opts.addOption(OptionBuilder.hasArg(true).isRequired(false)
				.withDescription("la|SunShine [default set to la]").withLongOpt("cluster").create("c"));
		
		PosixParser parser = null;
		CommandLine cmd = null;
		String clustername = null;
		try{
			parser = new PosixParser();
			cmd = parser.parse(opts, args);
			
			clustername = cmd.getOptionValue("c");
		}catch(Exception e){
			HelpFormatter helpformat = new HelpFormatter();
			helpformat.printHelp("Inference", opts);
			e.printStackTrace();
			System.exit(1);
		}
		
		if(clustername==null){
			clustername = "la";
		}
		if(clustername.equals("la")){
System.err.println("cluster: la");			
			conf.clear();
			conf.set("hadoop.client.ugi","webpdm,Sogou-RD@2008");
			conf.addResource("conf/core-site-la.xml");
			conf.addResource("conf/mapred-site-la.xml");
			conf.addResource("conf/inference.xml");
			conf.set("fs.path.inference.home", conf.get("fs.path.inference.home.la"));
		}else if(clustername.equals("SunShine")){
System.err.println("cluster: SunShine");
			conf.clear();
			conf.set("hadoop.client.ugi","web_research,9d1401302a959d80685c80bbea98906e");
			conf.addResource("conf/core-site-sunshine.xml");
			conf.addResource("conf/mapred-site-sunshine.xml");
			conf.addResource("conf/inference.xml");
			conf.set("fs.path.inference.home", conf.get("fs.path.inference.home.sunshine"));
		}else{
			System.err.println("cluster.name error");
			System.exit(1);
		}
		try {
			//conf.dumpConfiguration(conf, new PrintWriter(System.out));
			conf.writeXml(new PrintWriter(System.out));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Configuration conf = null;
	private Tool tInitializer = null;
	private Tool tInference = null;
	private Tool tReservedProcess = null;
	private Tool tReservedInference = null;
	private Tool tMessUp = null;
	private Tool tMessClean = null;
	
	public int run(String[] args) throws Exception{
		
		conf = this.getConf();
		getConf(conf, args);
		copyFromLocal(conf);
		copyInitRank(conf);
		copyAddWord(conf);
		copyBlackpv(conf);
		copyWordpv(conf);
		copyWeightpv(conf);
		copyVideoRank(conf);
		copyQueryPv(conf);
		
System.err.println("Initializer configuration:" + MyMR.getConfigureValue(conf,"initializer.classes"));
		tInitializer = new Initializer();
		ToolRunner.run(conf, tInitializer, args);

System.err.println("Inference configuration:" + MyMR.getConfigureValue(conf,"inference.classes"));
		tInference = new Inference();
		ToolRunner.run(conf, tInference, args);

System.err.println("Reserved Process configuration:" + MyMR.getConfigureValue(conf,"reservedprocess.classes"));
		tReservedProcess = new ReservedProcess();
		ToolRunner.run(conf, tReservedProcess, args);
		
System.err.println("Reserved Inference configuration:" + MyMR.getConfigureValue(conf,"reservedinference.classes"));
		tReservedInference = new ReservedInference();
		ToolRunner.run(conf, tReservedInference, args);
		
		reservedprocess(conf);
		exeShell(shellCommands);
		copyReservedOutput(conf);
		
		
System.err.println("Messup configuration:" + MyMR.getConfigureValue(conf,"messup.classes"));
		tMessUp = new MessUp();
		ToolRunner.run(conf, tMessUp, args);
		
System.err.println("Messclean configuration:" + MyMR.getConfigureValue(conf,"messclean.classes"));
		tMessClean = new MessClean();
		ToolRunner.run(conf, tMessClean, args);
		
		copyMergeToLocal(conf.get("fs.path.messclean.output"), "./Output/data.merge.inference", conf);
		return 0;
	}


		@SuppressWarnings("null")
		public static void main(String[] args) throws Exception{
			Tool tool = new Main();
			Configuration conf = HBaseConfiguration.create();
			System.out.println("Main Process:");
			ToolRunner.run(conf, tool, args);
		}

}
