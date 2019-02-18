package com.jacle.hive.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * hive2的API写ORC文件
 */
public class OrcWriterSelf {
    public static void main(String[] args)  throws  Exception
    {
        Path testFilePath = new Path("hdfs://m201:8020/output/test.orc");
        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<field1:int,field2:int,field3:int>");
        //指定文件的压缩方式
        Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.SNAPPY));
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];
        LongColumnVector second = (LongColumnVector) batch.cols[1];
        LongColumnVector third = (LongColumnVector) batch.cols[2];

        final int BATCH_SIZE = batch.getMaxSize();
        // add 1500 rows to file
        for (int r = 0; r < 15000000; ++r) {
            int row = batch.size++;
            first.vector[row] = r;
            second.vector[row] = r * 3;
            third.vector[row] = r * 6;
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }
}
