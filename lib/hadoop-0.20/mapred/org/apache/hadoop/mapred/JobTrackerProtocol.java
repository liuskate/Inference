package org.apache.hadoop.mapred;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSelector;
import java.util.Collection;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.io.IntWritable;

/** 
 * Protocol that a JobClient and the central JobTracker use to communicate.  The
 * JobClient can use these methods to submit a Job for execution, and learn about
 * the current system status.
 */ 
@KerberosInfo(
  serverPrincipal = JobTracker.JT_USER_NAME)
public interface JobTrackerProtocol extends VersionedProtocol {

  public static final long versionID = 1L;
  IntWritable activeTaskTrackersSize();
  IntWritable taskTrackersSize();
  IntWritable blackTaskTrackersSize();
  void updateUser();
}
