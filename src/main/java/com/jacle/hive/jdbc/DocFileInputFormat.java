package com.jacle.hive.jdbc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class DocFileInputFormat extends TextInputFormat
{
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        LineRecordReader lineRecordReader=new LineRecordReader(job,(FileSplit)genericSplit);

        return new DocRecordReader(lineRecordReader);
    }

    public class DocRecordReader implements RecordReader<LongWritable,Text>
    {
        private LineRecordReader lineRecordReader;
        private Text text;
        private String prefix="<DOC>";
        private String endPrefix="</DOC>";
        private StringBuilder strBuilder=new StringBuilder();

        public DocRecordReader(LineRecordReader reader)
        {
            this.lineRecordReader=reader;
            this.text=lineRecordReader.createValue();
        }

        @Override
        public boolean next(LongWritable returnKey, Text returnVal) throws IOException {

            while(lineRecordReader.next(returnKey,text))
            {
                if(text.toString().startsWith(prefix))
                {
                    //清空tmp string
                    strBuilder.setLength(0);
                }else if(text.toString().startsWith(endPrefix))
                {
                    returnVal.set(strBuilder.toString());
                    return true;

                }else
                {
                    //设定添加换行符的时机
                    if(strBuilder.length()!=0)
                    {
                        strBuilder.append("\n");
                    }
                    strBuilder.append(text);
                }
            }

            return false;
        }

        @Override
        public LongWritable createKey() {
            return lineRecordReader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return lineRecordReader.getPos();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return lineRecordReader.getProgress();
        }
    }

}
