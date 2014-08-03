package org.apache.hadoop.raid;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class BlockFixerDaemon {

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.raid.BlockFixerDaemon");

  /** Daemon thread to fix corrupt files */
  BlockFixer blockFixer = null;
  Daemon blockFixerThread = null;

  Configuration conf;
  boolean stopRequested;

  BlockFixerDaemon(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (Exception e) {
      this.stop();
      throw new IOException(e);
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      if (blockFixerThread != null) blockFixerThread.join();
    } catch (InterruptedException ie) {
      // do nothing
    }
  }

  /**
   * Stop blockfixer thread and wait for all to finish.
   */
  public void stop() {
    if (stopRequested) {
      return;
    }
    stopRequested = true;
    if (blockFixer != null) blockFixer.running = false;
    if (blockFixerThread != null) blockFixerThread.interrupt();
  }

  private void initialize(Configuration conf) 
    throws IOException, InterruptedException {
    this.conf = conf;

    try {
      this.blockFixer = BlockFixer.createBlockFixer(conf);
    } catch (ClassNotFoundException e) {
      System.err.println("[Error] Error happened in BlockFixerDaemon initialize: " + e);
    }
    this.blockFixerThread = new Daemon(this.blockFixer);
    this.blockFixerThread.start();
  }

  /**
   * Create an instance of the BlockFixerDaemon 
   */
  public static BlockFixerDaemon createBlockFixerDaemon(String argv[],
                                        Configuration conf) throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }

    Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
                                                DistributedFileSystem.class);
    conf.set("fs.hdfs.impl", clazz.getName());

    /*if (UserGroupInformation.isSecurityEnabled()) {
      String raidBlockfixerKeyTabPath = conf.get("raid.blockfixer.keytab.file", "/usr/lib/hadoop/conf/hdfs.keytab");
      String raidBlockfixerUserName = conf.get("raid.blockfixer.user.name", "hdfs");
      UserGroupInformation.loginUserFromKeytab(raidBlockfixerUserName, raidBlockfixerKeyTabPath);
    }*/

    BlockFixerDaemon node = new BlockFixerDaemon(conf);
    return node;
  }

  public static void main(String argv[]) throws Exception {
    try {
      BlockFixerDaemon blockFixerDaemon = createBlockFixerDaemon(argv, null);
      if (blockFixerDaemon != null) {
        blockFixerDaemon.join();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}

