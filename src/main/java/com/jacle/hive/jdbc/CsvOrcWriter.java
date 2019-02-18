package com.jacle.hive.jdbc;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.service.cli.thrift.TByteColumn;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * hive2的API写ORC文件
 * new API
 */
public class CsvOrcWriter {
    public static void main(String[] args) throws Exception {
        Path testFilePath = new Path("hdfs://m201:8020/output/testnew.orc");
        Configuration conf = new Configuration();
        //可以通过字符串来创建
//        TypeDescription schema = TypeDescription.fromString("struct<header1:int,header2:int,header3:int>");
        //也可以通过方法来创建
        TypeDescription schema = TypeDescription.createStruct().addField("header1", TypeDescription.createString())
                .addField("header2", TypeDescription.createString()).addField("header3",TypeDescription.createString());


/*        //输出ORC文件本地绝对路径
        String lxw_orc1_file = "/tmp/lxw_orc1_file.orc";
        Configuration conf = new Configuration();
        FileSystem.getLocal(conf);*/

        //指定文件的压缩方式
        Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.SNAPPY).stripeSize(67108864)
                .bufferSize(131072).blockSize(134217728));
        VectorizedRowBatch batch = schema.createRowBatch();

        List<String[]> dataList = getDataList("d:/hive_csv.csv");
        //默认是1024
        final int BATCH_SIZE = batch.getMaxSize();
        // add 1500 rows to file
        for (int r = 0; r < dataList.size(); ++r) {
            int row = batch.size++;
            for(int i=0;i<dataList.get(r).length;i++)
            {
                ((BytesColumnVector)(batch.cols[i])).setVal(row,dataList.get(r)[i].getBytes());
                if (row == BATCH_SIZE - 1) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }


    public static List<String[]> getDataList(String path) {
        ArrayList<String[]> dataList = new ArrayList<String[]>();

        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
//            CSVReader csvReader = new CSVReader(new InputStreamReader(in,"GBK"));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, "GBK"), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            String[] strs;
            while ((strs = csvReader.readNext()) != null) {
                dataList.add(strs);
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dataList;
    }
}
