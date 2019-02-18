package com.jacle.hive.jdbc;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * 有的需要自定义InputFormat
 * 例如hive的分隔符如果是多个字符的时候
 */
public class LogInputFormat extends TextInputFormat
{
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        LineRecordReader lineRecordReader= new LineRecordReader(job,(FileSplit)genericSplit,"###".getBytes());

        return new MyRecordReader(lineRecordReader);
    }


    //自定义字符段分隔符
    public class MyRecordReader implements RecordReader<LongWritable,Text>
    {
        private LineRecordReader recordReader;
        private Text text;

        public MyRecordReader(LineRecordReader reader)
        {
            this.recordReader=reader;
            this.text=reader.createValue();
        }


        //读取一行数据
        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            while(recordReader.next(key,text))
            {
                //获取整行的数据
                //数据读取进入text，value是最终返回的结果行
                Text desText=new Text(text.toString().replaceAll("%","\001"));
                value.set(desText.getBytes(),0,desText.getLength());

                return true;
            }


            return false;
        }

        @Override
        public LongWritable createKey() {
            return recordReader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return recordReader.getPos();
        }

        @Override
        public void close() throws IOException {
            recordReader.close();;
        }

        @Override
        public float getProgress() throws IOException {
            return recordReader.getProgress();
        }
    }

}
