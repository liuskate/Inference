package org.apache.hadoop.hdfs.server.namenode;

public class SnapshotManager {

  public static final String getSnapshotName(String filename){
    return "/snapshot" + filename;
  }
  
}
