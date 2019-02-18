package com.jacle.hive.jdbc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * hadoop1的时候创建ORC的API，属于就API
 * 这种方式是直接生成ORC文件，不能有效的进行文件的分片，可以看看后面如何通过mr来生成orc文件
 *
 * mr读取orc是不分serde（什么类来进行序列化的）
 */
public class CreateOrcFile {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        FileSystem fs = FileSystem.get(conf);
//        Path outputPath = new Path("hdfs://m201:8020/output/test.orc");

        StructObjectInspector inspector =(StructObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        OrcSerde serde = new OrcSerde();
        //hadoop2一定要使用mapreduce的包，不能使用mapred的包
        OutputFormat outFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        //根底执行还是需要加上hdfs前缀
        RecordWriter writer = outFormat.getRecordWriter(fs, conf, "hdfs://m201:8020/output4/myrow.orc", Reporter.NULL);
        writer.write(NullWritable.get(),
                serde.serialize(new MyRow("张三", 20), inspector));
        writer.write(NullWritable.get(),
                serde.serialize(new MyRow("李四", 22), inspector));
        writer.write(NullWritable.get(),
                serde.serialize(new MyRow("王五", 30), inspector));
        writer.close(Reporter.NULL);
        fs.close();
        System.out.println("write success .");

    }


    //继承Writable，MR的执行Unit
    static class MyRow implements Writable {
        String name;
        int age;

        MyRow(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            throw new UnsupportedOperationException("no write");
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            throw new UnsupportedOperationException("no read");
        }

    }
}
