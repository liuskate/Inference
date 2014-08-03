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
package org.apache.hadoop.security;

import java.io.IOException;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A simple congured file based implementation of {@link GroupMappingServiceProvider}.
 * 
 */
public class ConfigBasedGroupMapping implements GroupMappingServiceProvider,Configurable {
  
  private static final Log LOG = LogFactory.getLog(ConfigBasedGroupMapping.class);
  private Configuration conf;
  private Properties property;
  private Map<String, UserInfo> userCache;
  private long autoRefreshTimeout;
  private long freshTime;
  private String userInfoFilePath;

  private static class UserInfo {  
    public String password = null;
    public List<String> groups = null;
  }

  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
    autoRefreshTimeout = 
      conf.getLong("hadoop.security.auth.file.autofresh.secs", 30*60) * 1000;
    userInfoFilePath = conf.get("user.info.file");
    try {
      cacheGroupsRefresh();
    } catch (IOException e) {
      LOG.warn("setConf fail in ConfigBasedGroupMapping: " + e);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  private synchronized UserInfo getUserInfos(String userName) throws IOException{
    long now = System.currentTimeMillis();
    if ((freshTime == 0) || (autoRefreshTimeout + freshTime < now)) {
      freshTime = now;
      cacheGroupsRefresh();
    }

    UserInfo userInfos = userCache.get(userName);
    // if not hit the cache
    if (userInfos == null) {
      String value = (String)property.get(userName);

      if (value == null) {
        throw new IOException("No user: " + userName);
      }
      String[] passwdGroups = value.split(",");
      if (passwdGroups.length == 0) {
        throw new IOException("user passwd info wrong, user name: " + userName);
      }
      userInfos = new UserInfo();
      userInfos.password = passwdGroups[0].trim();

      userInfos.groups = new ArrayList<String>();
      for(int i = 1; i < passwdGroups.length; i++){
        userInfos.groups.add(passwdGroups[i].trim());
      }

      userCache.put(userName, userInfos);
    }

    return userInfos;
  }

  public String getPassword(String user) throws IOException {
    UserInfo userInfo = getUserInfos(user);
    return userInfo.password;
  }
  
  @Override
  public List<String> getGroups(String user) throws IOException {
    UserInfo userInfo = getUserInfos(user);
    return userInfo.groups;
  }

  @Override
  public synchronized void cacheGroupsRefresh() throws IOException {
    LOG.info("ConfigBasedGroupMapping refresh operation triggered, file name is " + userInfoFilePath);
    FileInputStream inputFile = null;
    try{
      Properties newPropertie = new Properties();
      //userInfoFilePath = conf.get("user.info.file");
      if (userInfoFilePath == null) {
        throw new IOException("user.info.file is NULL");
      }
      inputFile = new FileInputStream(userInfoFilePath);
      newPropertie.load(inputFile);
      this.property = newPropertie;
      userCache = new HashMap<String, UserInfo>();
    } catch (Exception e){
      LOG.warn(e.toString());
    } finally {
      if (inputFile != null) {
        try{
          inputFile.close();
        }catch (Exception e){}
      }
    }
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }
}
