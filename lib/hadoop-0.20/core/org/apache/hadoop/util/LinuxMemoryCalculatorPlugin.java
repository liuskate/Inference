/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

//import java.io.BufferedReader;
import java.io.FileNotFoundException;
//import java.io.FileReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Plugin to calculate virtual and physical memories on Linux systems.
 */
public class LinuxMemoryCalculatorPlugin extends MemoryCalculatorPlugin {
  private static final Log LOG =
      LogFactory.getLog(LinuxMemoryCalculatorPlugin.class);

  /**
   * proc's meminfo virtual file has keys-values in the format
   * "key:[ \t]*value[ \t]kB".
   */
  private static final String PROCFS_MEMFILE = "/proc/meminfo";
  private static final Pattern PROCFS_MEMFILE_FORMAT =
      Pattern.compile("^([a-zA-Z]*):[ \t]*([0-9]*)[ \t]kB");

  // We just need the values for the keys MemTotal and SwapTotal
  private static final String MEMTOTAL_STRING = "MemTotal";
  private static final String SWAPTOTAL_STRING = "SwapTotal";
  private static final String MEMFREE_STRING = "MemFree";
  private static final String BUFFERS_STRING = "Buffers";
  private static final String CACHED_STRING = "Cached";

  private long ramSize = 0;
  private long swapSize = 0;
  private long memFree = 0;
  private long buffers = 0;
  private long cached = 0;

  boolean readMemInfoFile = false;

  private void readProcMemInfoFile(boolean oneShortRead) {

    if ((oneShortRead) && (readMemInfoFile)) {
      return;
    }

    // Read "/proc/memInfo" file
    // Store all the file contents in memory at one time to avoiding the timming issue.
    byte[] filecontent = new byte[2048];
    boolean errorHappened = false;
    FileInputStream in = null;
    try {
      in = new FileInputStream(PROCFS_MEMFILE);
      int realFileSize = in.read(filecontent);
      if (realFileSize == 2048) {
        LOG.warn("Strang thing happended: meminfo file has the size 2048!");
      }
    } catch (Exception e) {
      // shouldn't happen....
      e.printStackTrace();
      errorHappened = true;
    } finally {
      try {
        in.close();
      } catch (Exception i) {
        LOG.warn("Error closing the stream " + i);
      }
    }

    if (errorHappened == true) {
      return;
    }

    Matcher mat = null;

    try {
      String fileStr = new String(filecontent);
      StringTokenizer strToken = new StringTokenizer(fileStr,"\n");
      while(strToken.hasMoreTokens()) {
        String str = strToken.nextToken();

        mat = PROCFS_MEMFILE_FORMAT.matcher(str);
        if (mat.find()) {
          if (mat.group(1).equals(MEMTOTAL_STRING)) {
            ramSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(SWAPTOTAL_STRING)) {
            swapSize = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(MEMFREE_STRING)) {
            memFree= Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(BUFFERS_STRING)) {
            buffers = Long.parseLong(mat.group(2));
          } else if (mat.group(1).equals(CACHED_STRING)) {
            cached = Long.parseLong(mat.group(2));
          }
        }
      }
    } catch (Exception io) {
      LOG.warn("Error reading the stream " + io);
    }

    readMemInfoFile = true;
  }

  /** {@inheritDoc} */
  @Override
  public long getPhysicalMemorySize() {
    readProcMemInfoFile(true);
    return ramSize * 1024;
  }

  @Override
  public long getFreeMemorySize() {
    readProcMemInfoFile(false);
    return (memFree + cached) * 1024;
  }

  /** {@inheritDoc} */
  @Override
  public long getVirtualMemorySize() {
    readProcMemInfoFile(true);
    return (ramSize + swapSize) * 1024;
  }

  /**
   * Test the {@link LinuxMemoryCalculatorPlugin}
   * 
   * @param args
   */
  public static void main(String[] args) {
    LinuxMemoryCalculatorPlugin plugin = new LinuxMemoryCalculatorPlugin();
    System.out.println("Physical memory Size(bytes) : "
        + plugin.getPhysicalMemorySize());
    System.out.println("Total Virtual memory Size(bytes) : "
        + plugin.getVirtualMemorySize());
  }
}
