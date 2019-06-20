package com.jacle.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DBMr
{
    public static class DBMapper extends Mapper<LongWritable,PersonReader,LongWritable,Text>
   {
       @Override
       protected void map(LongWritable key, PersonReader value, Context context) throws IOException, InterruptedException {
           context.write(new LongWritable(value.getId()),new Text(value.toString()));
       }
   }

   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       Configuration conf=new Configuration();
       DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://10.1.12.201:3306/test", "root", "1234@abcd");

       Job job=Job.getInstance(conf,"dbReader");
       job.setInputFormatClass(DBInputFormat.class);
       job.setMapperClass(DBMapper.class);

       job.setOutputKeyClass(LongWritable.class);
       job.setOutputValueClass(Text.class);

       DBInputFormat.setInput(job, PersonReader.class, "select id,name,age from t_person", "select count(1) from t_person");

       String path="hdfs://10.1.12.201:8020/outputdb";
       FileSystem fs= FileSystem.get(conf);
       Path p=new Path(path);
       if(fs.exists(p)){
           fs.delete(p, true);
           System.out.println("输出路径存在，已删除！");
       }
       FileOutputFormat.setOutputPath(job,p );
       job.setJarByClass(DBMr.class);
       boolean jobFlag=job.waitForCompletion(true);
       System.exit(jobFlag?0:1);
   }
}
