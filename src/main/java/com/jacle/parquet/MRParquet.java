package com.jacle.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import java.io.IOException;

/**
 * 通常实际的开发过程中使用分布式的mr来进行parquet的生成效率更高
 */
public class MRParquet
{

    //MR读取parquet文件
    //最好都创建静态内部类，在调用的时候方便
    public static class ParquetMapper extends Mapper<LongWritable,Group,NullWritable,Text>
    {
        @Override
        protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),new Text(value.toString()));
        }
    }

    //最好都创建静态内部类，在调用的时候方便
    public static class MyReadSupport extends DelegatingReadSupport
    {
        public MyReadSupport() {
            super(new GroupReadSupport());
        }

        @Override
        public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
            return super.init(context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String readSchema = "";
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MRParquet.class);
        job.setJobName("parquet");

        String in = "hdfs://10.1.12.201:8020/user/hive/warehouse/t_parquet/000000_0";
        String  out = "hdfs://10.1.12.201:8020/user/jijj/parquet";

        job.setMapperClass(ParquetMapper.class);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, MyReadSupport.class);
        ParquetInputFormat.addInputPath(job, new Path(in));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setNumReduceTasks(0);


        //判断output文件夹是否存在，如果存在则删除
        Path path = new Path(out);
        //根据path找到这个文件
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            System.out.println(out+"已存在，删除");
        }

        job.waitForCompletion(true);

    }
}
