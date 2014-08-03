<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.lang.Integer"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  TaskTracker tracker = (TaskTracker) application.getAttribute("task.tracker");
  String trackerName = tracker.getName();
%>

<html>
<head>
<title><%= trackerName %> Task Tracker Status</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>
<body>
<h1><%= trackerName %> Task Tracker Status</h1>
<img src="/static/hadoop-logo.jpg"/><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>

<h2>Running tasks</h2>
<center>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
    <td>Progress</td><td>Errors</td></tr>

  <%
     Iterator itr = tracker.getRunningTaskStatuses().iterator();
     while (itr.hasNext()) {
       TaskStatus status = (TaskStatus) itr.next();
       out.print("<tr><td>" + status.getTaskID());
       out.print("</td><td>" + status.getRunState()); 
       out.print("</td><td>" + 
                 StringUtils.formatPercent(status.getProgress(), 2));
       out.print("</td><td><pre>" +
           HtmlQuoting.quoteHtmlChars(status.getDiagnosticInfo()) +
           "</pre></td>");
       out.print("</tr>\n");
     }
  %>
</table>
</center>

<h2>Non-Running Tasks</h2>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
  <%
    for(TaskStatus status: tracker.getNonRunningTasks()) {
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      out.print("<td>" + status.getRunState() + "</td></tr>\n");
    }
  %>
</table>


<h2>Tasks from Running Jobs</h2>
<center>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
    <td>Progress</td><td>Errors</td></tr>

  <%
     itr = tracker.getTasksFromRunningJobs().iterator();
     while (itr.hasNext()) {
       TaskStatus status = (TaskStatus) itr.next();
       out.print("<tr><td>" + status.getTaskID());
       out.print("</td><td>" + status.getRunState()); 
       out.print("</td><td>" + 
                 StringUtils.formatPercent(status.getProgress(), 2));
       out.print("</td><td><pre>" +
           HtmlQuoting.quoteHtmlChars(status.getDiagnosticInfo()) +
           "</pre></td>");
       out.print("</tr>\n");
     }
  %>
</table>
</center>

<h2>Physical Memory Usage Threshold</h2>
<select id="sel" onChange="window.location = '/tasktracker.jsp?memoryusagethreshold=<CHOICE>'.replace('<CHOICE>', document.getElementById('sel').value);">
  <%
    String memoryUsageStr = request.getParameter("memoryusagethreshold");
    if ((memoryUsageStr != null) && (!memoryUsageStr.equals(""))) {
      tracker.setMemoryUsageThreshold(Integer.parseInt(memoryUsageStr));
    }
    int memoryThreshold = tracker.getMemoryUsageThreshold();
    String selected = "";
    for (int i = 1; i < 101; i ++) {
      selected = i == memoryThreshold ? " selected=\"selected\"" : "";
      out.print("<option value=\"" + i + "\"" + selected + ">"+i+"</option>\n");
    }
  %>
</select>

<h2>Local Logs</h2>
<a href="/logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>
