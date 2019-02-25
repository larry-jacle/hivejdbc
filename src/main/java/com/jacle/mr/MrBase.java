package com.jacle.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * mr base
 */
public class MrBase {

    private static Logger logger=Logger.getLogger(MrBase.class);

    public static void main(String[] args) throws Exception
    {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);

        //通过GenericOptionParser来解析参数
        //直接通过args来获取是不对的
        for(int index=0;index<args.length;index++)
        {
            logger.info("index-->"+index+":"+args[index]);
        }
        String[] needArgs=new GenericOptionsParser(conf,args).getRemainingArgs();

        //设定run class
        job.setJarByClass(MrBase.class);
        job.setMapperClass(MrMapper.class);
        job.setReducerClass(MrReducer.class);

        //设定map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设定reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定MR的输入输出路径
        FileInputFormat.addInputPath(job,new Path(needArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(needArgs[1]));

        //true表示运行信息显示给客户端，false只是等待任务结束
        //mr的运行是异步的，这一句一定要加
        boolean jobFlag=job.waitForCompletion(true);
        System.exit(jobFlag?0:1);

    }


    private static class MrMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //默认的输入的key是偏移量
            String[] strs=value.toString().split(",");
            for(String s:strs)
            {
                context.write(new Text(s),new IntWritable(1));
            }
        }
    }

    private static class MrReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;

            for(IntWritable v:values)
            {
               sum=sum+v.get();
            }

            context.write(key,new IntWritable(sum));
        }
    }

}
