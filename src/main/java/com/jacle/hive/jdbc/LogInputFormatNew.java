package com.jacle.hive.jdbc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 新API无法使用TextInputFormat，报错：FAILED: SemanticException 1:14 Input format must implement InputFormat. Error encountered near token 'hive_selfinput'
 */
public class LogInputFormatNew extends TextInputFormat
{
    @Override
    //这个方法是这是行分隔符
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new LineRecordReader("###".getBytes());
    }

}
