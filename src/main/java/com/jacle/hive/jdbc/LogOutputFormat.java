package com.jacle.hive.jdbc;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;


/**
 * hive的outputformat
 * 使用场景，将输出的文件进行归类，指定的文件记录合并到指定的文件
 * 当然这种更佳适合的方法应该使用filter更好
 */
public class LogOutputFormat extends TextOutputFormat implements HiveOutputFormat
{

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path path, Class aClass, boolean b, Properties properties, Progressable progressable) throws IOException {
        FileSystem fileSystem=path.getFileSystem(jobConf);
        FSDataOutputStream fsDataOutputStream=fileSystem.create(path);

        return new MyWriter(fsDataOutputStream);
    }

    public class MyWriter implements FileSinkOperator.RecordWriter
    {
        private FSDataOutputStream out;

        public MyWriter(FSDataOutputStream outputStream)
        {
            this.out=outputStream;
        }


        @Override
        public void write(Writable wr) throws IOException {
            String destStr=wr.toString();
            out.write(destStr.getBytes(),0,destStr.length());

        }

        @Override
        public void close(boolean b) throws IOException {
            out.flush();
            out.close();
        }

    }
}
