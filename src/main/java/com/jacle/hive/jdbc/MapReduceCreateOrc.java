package com.jacle.hive.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import java.io.IOException;


/**
 * mapreduce新API 创建ORC文件
 * 通过ORC创建ORC
 */
public class MapReduceCreateOrc {
    private static Logger logger=Logger.getLogger(MapReduceCreateOrc.class);

    //读取orc的一行的内容
    public static class ORCMapper extends
            Mapper<NullWritable, OrcStruct, Text, Text> {
        public void map(NullWritable key, OrcStruct value, Context context)
                throws IOException, InterruptedException {
            String strBuffer = "";
            for (int i = 0; i < value.getNumFields(); i++) {
                strBuffer = strBuffer + value.getFieldValue(i);
                logger.info(value.getFieldValue(i));
            }

            context.write(new Text(strBuffer),new Text("1"));
        }
    }

    //写入到另外的orc文件
    public static class ORCReducer extends
            Reducer<Text, Text, NullWritable, OrcStruct> {
        private TypeDescription schema = TypeDescription
                .fromString("struct<data:string,countstr:string>");
        //通过定义schema来创建orc的schema格式
        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        //null可写对象使用静态方法get来直接获取
        private final NullWritable nw = NullWritable.get();

        //reduce的value是一个迭代器，可知进行了融合
        public void reduce(Text key, Iterable<Text> values, Context output)
                throws IOException, InterruptedException {
            for (Text val : values) {
                pair.setFieldValue(0, key);
                pair.setFieldValue(1, val);
                output.write(nw, pair);
            }
        }
    }

    public static void main(String args[]) throws Exception {

        Configuration conf = new Configuration();
        conf.set("orc.mapred.output.schema","struct<name:string,mobile:string>");
        Job job = Job.getInstance(conf, "ORC Test");
        job.setJarByClass(MapReduceCreateOrc.class);
        job.setMapperClass(ORCMapper.class);
        job.setReducerClass(ORCReducer.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setOutputFormatClass(OrcOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://m201:8020/output"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://m201:8020/output_neworc"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
