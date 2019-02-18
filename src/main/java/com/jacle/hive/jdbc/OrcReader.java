package com.jacle.hive.jdbc;

import com.jacle.mr.MrBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 读取ORCfile
 */
public class OrcReader {
    private static Logger logger = Logger.getLogger(OrcReader.class);

    public static class OrcReaderMapper extends Mapper<NullWritable, OrcStruct, Text, IntWritable> {
        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

            String strBuffer = "";
            for (int i = 0; i < value.getNumFields(); i++) {
                strBuffer = strBuffer + value.getFieldValue(i);
                logger.info(value.getFieldValue(i));
            }

            context.write(new Text(strBuffer),new IntWritable(1));
        }
    }

    public static void main(String[] args) throws Exception {
        //通过mapreduce来读取orcfile
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("reading orc");

        //设定run class
        job.setJarByClass(OrcReader.class);
        job.setMapperClass(OrcReaderMapper.class);

/*        //设定map的输出类型方式1
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设定reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/

        //设定map的输出类型方式2
        job.setInputFormatClass(OrcInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //如果job只有map的情况，要指定reduce为0，否则无法产生结果文件
        job.setNumReduceTasks(0);

        //指定MR的输入输出路径
        OrcInputFormat.addInputPath(job,new Path("hdfs://m201:8020/output"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://m201:8020/output4"));

        //true表示运行信息显示给客户端，false只是等待任务结束
        //mr的运行是异步的，这一句一定要加
        boolean jobFlag = job.waitForCompletion(true);
        System.exit(jobFlag ? 0 : 1);


    }
}
