# QST_Hadoop_Project

## 项目背景

某电商平台XD，每天有大量的用户访问，XD的产品经理ZB，想了解现在网站的访问情况，于是提出了一个需求

1. 需要看到网站的访问情况
    1. 包括PV、UV
2. 需要符合大数据的概念
3. 必须支持后的开发需求，要尽量高的开发效率

## 项目开展

本次项目的负责人是：

项目分4个阶段开展

## Round 1

本回合完成任务主要完成：

1. 完成设计方案
    1. 方案内容填写在./Round1/README.md 文件里
    2. 方案包含所有操作的内容，由环境搭建，到最后的代码运行
2. 完成环境搭建
    1. 建议进行操作一遍
3. 完成以下统计需求
    1. 完成每天的UV统计
    2. 完成每天访问量Top10的Show统计
    3. 完成每天的次日留存统计

相关资料完成后，请发起pull request，里面标题填上自己的名字。

数据在客户端本地磁盘 /home/hadoop/qst/ray/2015-1\* ， 在HDFS上也有 /user/hadoop/hadoop_project/*

---

## Round 2 - Start From the Beginning

有一家电商公司，委托了我们来做一个项目，项目目标是搭建一个日志处理系统，定期地收集日志，并且对日志进行统计分析，产出对该网站的统计报表。并且提供一些线上的。

现在陈经理带领大家一块来进行这个项目。首先，大家的技术都是Hadoop相关的，所以我们这一次做的项目，也是用Hadoop的技术来实现。

---
我们的实施过程，会划分为几个部分

1. 需求摸底：了解清楚这个日志统计平台，需要的是什么样的功能
2. 方案设计：根据我们接触到的需求，我们进行方案设计，让对方知道我们的系统，是由什么组成，怎么工作，如何提供服务的。
3. 项目拆解：根据方案，我们把这个项目，分解成多个小项目，进行时间安排。
4. 项目实施：根据拆解的内容，我们一一进行编码，实现每个小项目。
5. 项目验收：对我们的系统进行验收，由对方判断是否满足他们的需求。

---
由于陈经理已经做好了客户的沟通，『需求摸底』已经完成了。我们从『方案设计』开始。

以下是对方的需求：

1. 必须能安全地存储这些日志，不能够丢失。
2. 需要对这些日志进行统计，得到网站的访问情况。
3. 需要提供实时的查询服务，提供给外部的服务使用。

其中，统计需求有以下这些：

1. 统计每日、周、月的UV数量。
2. 统计每日的PV量。
3. 统计次日留存、次月留存。
4. 统计每类网页的跳转率。
5. 统计每天从baidu跳转过来的PV
6. 统计每天iOS和Android的UV数


实时的查询服务需求：

1. 查询当前的show的访问数量
2. 查询当前的musician的访问数量

---

以下，请大家完成剩下的部分，陈经理会回答过程中的问题。请大家前面所提到的：

* 方案设计——提交方案，方案中包括系统如何设计，包含哪些模块，为什么选用这些模块。
* 项目拆解——把需求安排在一定的时间内完成，给出项目各个阶段和小项目的完成时间。
* 项目实施——实现需求，把项目的实施代码，上传到github。

                      电商日志后台处理系统
一，	模块划分
1.	日志采集模块：负责将各个前端的web服务器的日志传送到日志接收节点上。然后再将这些日志导入hdfs中
2.	数据预处理：把采集回来的日志数据利用mapreduce进行清洗出来，保留有用的信息
3.	存储处理模块：hadoop的hdfs/hbase用来存储日志与分析后的数据。
4.	查询分析模块：主要用hive等mapreduce任务进行分析，把分析结果存在mysql等关系型数据库中
5.	结果输出模块：根据用户的查询请求，将查询的结果显现出来，供用户查看
二，	系统执行流程
1.	系统运行流程图：
 
三，	项目拆解
1.	数据预处理与存储-2016-12.08-2016.12.10
1）	对日志文件进行正则切割，抽取ip ,时间 url 用户当前页面 页面类别
2）	建立hive表，以日期与类别为分区，
3）	还要把数据存储在hbase上，可以进行实时的查询，但为了跟页面需要主要是统计类别的访问数量，所以可以把rowkey设置是时间+页面类别，
2.	数据分析-2016-12-11-2016-12-13
1）	编写hivesql语句，进行统计每天，周，月的uv数量，把分析结果导进mysql的uv表
2）	编写sql语句，进行统计每日的pv量，导进mysql的pv表中
3）	编写sql统计次日留存，次月留存，导进mysql的留存表
4）	统计每天每类页面的跳转率，导进mysql的跳转率表
5）	统计每天从百度跳转过来的pv，导进mysql百度表中
6）	统计每天ios与Android的uv数，导进mysql的用户手机表
3.	实时查询的服务需求
1）	查询当前的show的访问量：利用hbase的javaAPI进行查询，hbase，启动一个java程序就可以实时查询到当前的show的访问量
2）	查询当前的musician的访问数量：利用hbase的javaAPI进行查询，hbase，启动一个java程序就可以实时查询到当前的musician的访问量
3）	
4.	
四．代码
1.数据清洗代码：
package Log_Hadoop_shizhan;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogClean {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(LogClean.class);
		 
		job.setMapperClass(map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
		job.waitForCompletion(true);
	}
	public static class map extends Mapper<LongWritable, Text, Text, Text>{
         private LogParser logparser=new LogParser();
	     private Text map_key=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String log=value.toString();
			String[]user=logparser.parse(log);
			map_key.set(user[0]+"\t"+user[1]+"\t"+user[2]+"\t"+user[3]+"\t"+user[4]);
			context.write(map_key, new Text(""));
		}
		
	}
	public static class LogParser {
		public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");

		/**
		 * 解析日志的行记录
		 * @param line
		 * @return 数组含有5个元素，分别是ip、时间、url、手机系统、跳转是否是百度
		 */
		
		public String[] parse(String line){
			String ip = parseIP(line);
			String time;
			try {
				time = parseTime(line);
			} catch (Exception e1) {
				time = "null";
			}
			String url;
			try {
				url = parseURL(line);
			} catch (Exception e) {
				url = "null";
			}
		     String os=parseOS(line);
		     String baidu=parseDangPage(line);
		
			
			return new String[]{ip, time ,url,os,baidu};
		}
		private String parseDangPage(String line){//当前页面
			String url;
			try{
			final int first = line.lastIndexOf("http://www.baidu.com");
			final int last = line.lastIndexOf(")");
			url = line.substring(first, last);
			url="baidu";
			}catch(Exception e){
				url="null";
			}
			return url;
			
		}
		private String parseOS(String line) {
			String os;
			try{
			if(line.contains("Android")){
				os="Android";
			}else if(line.contains("iPhone")){
				os="iPhone";
			}else{
				os="null";
			}
			
			}catch(Exception e){
				os="null";
			}
			return os;
		}
	
		private String parseURL(String line) {
			String pattern="(\\d+\\.\\d+\\.\\d+\\.\\d+).{5}\\[(\\d+\\/[a-zA-Z]+\\/\\d+\\:\\d+\\:\\d+\\:\\d+).{13}\\/((show|musicians).\\d+)";  
			Pattern r = Pattern.compile(pattern);
			 Matcher m = r.matcher(line);
			String url = null;
			 try{
				if (m.find()){
					url=m.group(3);
				}
			 }catch(Exception e){
				 url="null";
			 }
			return url;
		}
		private String parseTime(String line) {
			final int first = line.indexOf("[");
			final int last = line.indexOf("+0800]");
			String time = line.substring(first+1,last).trim();
			try {
				return dateformat1.format(FORMAT.parse(time));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return "";
		}
		private String parseIP(String line) {
			String ip = line.split("- -")[0].trim();
			return ip;
		}
		
	}
}
批量导入hbase程序：
package Hbase_sqoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



public class Hbase {
public static void main(String[] args) throws Exception {
	final Configuration configuration = new Configuration();
	//设置zookeeper
	configuration.set("hbase.zookeeper.property.clientport", "2181");
	configuration.set("hbase.zookeeper.quorum", "vm10-0-0-2.ksc.com");
	//设置hbase表名称
	configuration.set(TableOutputFormat.OUTPUT_TABLE, "Hbase_log");
	//将该值改大，防止hbase超时退出
	configuration.set("dfs.socket.timeout", "180000");
	
	final Job job = new Job(configuration, "HBaseBatchImport");
	
	job.setMapperClass(map.class);
	job.setReducerClass(reduce.class);
	//设置map的输出，不设置reduce的输出类型
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setInputFormatClass(TextInputFormat.class);
	//不再设置输出路径，而是设置输出格式类型
	job.setOutputFormatClass(TableOutputFormat.class);
	
	FileInputFormat.setInputPaths(job, "hdfs://vm10-0-0-2.ksc.com:9000/user/hadoop/hadoop_project/*");
	
	job.waitForCompletion(true);

}
public static class map extends Mapper<LongWritable, Text, LongWritable, Text>{
	 private Text v2 = new Text();
	 private LogParser logparser=new LogParser();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String log=value.toString();
		String[]user=logparser.parse(log);//String[]{ip, time ,url,os,baidu};
		try {
			String rowKey = user[1]+"/"+user[2];
			String val=user[0]+"\t"+user[1]+"\t"+user[2]+"\t"+user[3]+"\t"+user[4];
			v2.set(rowKey+"\t"+val);
			context.write(key, v2);
		} catch (NumberFormatException e) {
			final Counter counter = context.getCounter("BatchImport", "ErrorFormat");
			counter.increment(1L);
			System.out.println("出错了"+user[0]+" "+e.getMessage());
		}
	}
}
public static class reduce extends TableReducer<LongWritable, Text, NullWritable>{
	public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
		Context context)
			throws IOException, InterruptedException {
		Date data = null;
		for (Text text : values) {
			final String[] splited = text.toString().split("\t");
			  try {
				data=dateformat1.parse(splited[2]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			final Put put = new Put(Bytes.toBytes(splited[0]),(data.getTime())/1000);//row ,ip, time ,url,os,baidu
			put.add(Bytes.toBytes("info"), Bytes.toBytes("ip"), Bytes.toBytes(splited[1]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(splited[3]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("systems"), Bytes.toBytes(splited[4]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("page"), Bytes.toBytes(splited[5]));
			context.write(NullWritable.get(), put);
		}

	}
	
}
public static class LogParser {
	public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");

	/**
	 * 解析日志的行记录
	 * @param line
	 * @return 数组含有5个元素，分别是ip、时间、url、手机系统、跳转是否是百度
	 */
	
	public String[] parse(String line){
		String ip = parseIP(line);
		String time;
		try {
			time = parseTime(line);
		} catch (Exception e1) {
			time = "null";
		}
		String url;
		try {
			url = parseURL(line);
		} catch (Exception e) {
			url = "null";
		}
	     String os=parseOS(line);
	     String baidu=parseDangPage(line);
	
		
		return new String[]{ip, time ,url,os,baidu};
	}
	private String parseDangPage(String line){//当前页面
		String url;
		try{
		final int first = line.lastIndexOf("http://www.baidu.com");
		final int last = line.lastIndexOf(")");
		url = line.substring(first, last);
		url="1";
		}catch(Exception e){
			url="0";
		}
		return url;
		
	}
	private String parseOS(String line) {
		String os;
		try{
		if(line.contains("Android")){
			os="Android";
		}else if(line.contains("iPhone")){
			os="iPhone";
		}else{
			os="other";
		}
		
		}catch(Exception e){
			os="other";
		}
		return os;
	}

	private String parseURL(String line) {
		String pattern="(\\d+\\.\\d+\\.\\d+\\.\\d+).{5}\\[(\\d+\\/[a-zA-Z]+\\/\\d+\\:\\d+\\:\\d+\\:\\d+).{13}\\/((show|musicians|musician)).\\d+";  
		Pattern r = Pattern.compile(pattern);
		 Matcher m = r.matcher(line);
		String url = "other";
		 try{
			if (m.find()){
				url=m.group(3);
			}
		 }catch(Exception e){
			 url="other";
		 }
		return url;
	}
	private String parseTime(String line) {
		final int first = line.indexOf("[");
		final int last = line.indexOf("+0800]");
		String time = line.substring(first+1,last).trim();
		try {
			return dateformat1.format(FORMAT.parse(time));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return "";
	}
	private String parseIP(String line) {
		String ip = line.split("- -")[0].trim();
		return ip;
	}
	
}
}
创建hive表语句：
  
create external table log_yuxingsen (ip string , times string,url string ,os string ,baidu string ) partitioned by (putdate string ) row format delimited fields terminated by '\t' ;
load data inpath '/user/hadoop/yuxingsen/d2/part-r-00000'  into table log_yuxingsen partition (putdate='2015-10-02');
//统计uv
select count(distinct(ip))from log_yuxingsen where putdate='2015-10-02';
//统计pv
select count(ip)from log_yuxingsen where putdate='2015-10-01';
//统计使用手机系统是ios
select count(ip) from log_yuxingsen where putdate='2015-10-01' and os='iPhone';
select count(ip)from log_yuxingsen where putdate='2015-10-01' and baidu='baidu';
统计次日留存：
SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT ip  FROM  log_yuxingsen  WHERE putdate='2015-10-01')t ,(SELECT DISTINCT ip  FROM  log_yuxingsen  WHERE putdate='2015-10-02') b WHERE t.ip=b.ip)/(SELECT COUNT(*) FROM( SELECT DISTINCT ip FROM  log_yuxingsen WHERE putdate='2015-10-01')h) ;
页面跳转率程序：
 
package Log_Hadoop_shizhan;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import  org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Log_Hadoop_shizhan.LogClean.map;
public class TiaoZhuanLv {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(TiaoZhuanLv.class);
		
		job.setMapperClass(map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setCombinerClass(combine.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
		job.waitForCompletion(true);
		
		Counters counters=job.getCounters();
		Counter counte1=counters.findCounter("ddd2","show_count");
		Counter counte2=counters.findCounter("ddd1","music_count");
		Counter counte3=counters.findCounter("ddd3","music_zhuan");
		Counter counte4=counters.findCounter("ddd4","show_zhuan");
		double show=(double)counte4.getValue()/counte1.getValue();
		double music=(double)counte3.getValue()/counte2.getValue();
		System.out.println("show_tiaozhuan"+show);
		System.out.println("music_tiaozhuan"+music);
	}
	public static class map extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str=value.toString();
			String []user=str.split("\t");
			context.write(new Text(user[0]), new Text(user[2]+"\t"+user[1]));
		}
	}
	public static class combine extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
	        for(Text text:values){
	        	String log=text.toString();
	        	if(log.contains("show")||log.contains("music")){
	        		context.write(key, text);
	        	}
	        }
		}
		
	}
	public static class reduce extends Reducer<Text, Text, Text, Text>{
		 private long count1=0;
		 private long count2=0;
		 private long count3=0;
		 private long count4=0;
         private ArrayList<Date> show_list=new ArrayList<Date>();
         private ArrayList<Date> music_list=new ArrayList<Date>();
         private static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");
		@Override
		protected void reduce(Text key, Iterable<Text> values,
			       Context context)
				throws IOException, InterruptedException {
		
	       for(Text value:values){
	    	   String time_lei=value.toString();
	    	   String[]user=time_lei.split("\t");
	    	   try {
		    	   Date date=dateformat1.parse(user[1]);
		    	   if(time_lei.startsWith("show")){
		    		   count1++;
						show_list.add(date);
		    		  
		    	   }else{
		    		   count2++;
		    		   music_list.add(date);
		    	   }
	    	   } catch (ParseException e) {
					e.printStackTrace();
	    		   }
	       }
	       Calendar cal = Calendar.getInstance();   
	        Date dateMax;
	        show:for(Date date_show:show_list){
	        	for(Date date_music:music_list){
	        		  cal.setTime(date_show);   
	      	        cal.add(Calendar.MINUTE, 1);// 24小时制   
	      	        dateMax = cal.getTime();   
	        		if(date_show.getTime()<date_music.getTime()&&date_music.getTime()<dateMax.getTime()){
	        			count3++;
	        			break show;
	        		}
	        	}
	        }
	       
	        music:for(Date date_show:music_list){
	        	for(Date date_music:show_list){
	        		  cal.setTime(date_show);   
	      	        cal.add(Calendar.MINUTE, 1);// 24小时制   
	      	        dateMax = cal.getTime();   
	        		if(date_show.getTime()<date_music.getTime()&&date_music.getTime()<dateMax.getTime()){
	        			count4++;
	        			break music;
	        		}
	        	}
	        	context.write(key, new Text(count3+""));
	        }

		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Counter count_1=context.getCounter("ddd1", "music_count");
			count_1.increment(count2);
			Counter count_2=context.getCounter("ddd2", "show_count");
			count_2.increment(count1);
			Counter count_3=context.getCounter("ddd3", "music_zhuan");
			count_3.increment(count4);
			Counter count_4=context.getCounter("ddd4", "show_zhuan");
			count_4.increment(count3);
		}
		
	}
}
实时查询show的程序：
package Hbase_sqoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class show_hbase {
	private static Configuration conf=null;
	private static HBaseAdmin admin=null;
	private static Logger log=Logger.getLogger(show_hbase.class);
	private static String hTableName = "Hbase_log";

	
	  @SuppressWarnings("deprecation")	
    public static void main( String[] args ) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
    {
        initLog();
        conf=new Configuration();
        conf.set("hbase.zookeeper.property.clientport", "2181");
    	conf.set("hbase.zookeeper.quorum", "master");//,192.168.2.34,192.168.2.45,192.168.2.44
    	admin = new HBaseAdmin(conf);
    	log.info("Log in");
    	System.out.println(showcount());
    	admin.close();
    }

    private static int showcount() throws IOException{
    	if(tableExist(hTableName)==false){
    		return 0;
    	}
    	int count=0;
    	//通过scan查询数据
    	HTable table=new HTable(conf, Bytes.toBytes(hTableName));
    	 Scan scan=new Scan();
    	 scan.addColumn("info".getBytes(), "url".getBytes());
    	 ValueFilter fileter=new ValueFilter(CompareFilter.CompareOp.EQUAL,                     
    		        new SubstringComparator("show"));
    	 scan.setFilter(fileter);
    	 ResultScanner results = table.getScanner(scan);
    	 Result result=null;
    	while( (result=results.next())!=null){
    		count++;
    	}
       
         return count;
    }
	
	
    private static boolean tableExist(String tableName) throws IOException{
		//判断表是否存在
		return admin.tableExists(tableName);	
	}
    
    private static void initLog(){
    	BasicConfigurator.configure();
    	log.setLevel(Level.INFO);
    }
}




