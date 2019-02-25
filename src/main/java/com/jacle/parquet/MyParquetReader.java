package com.jacle.parquet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
//import org.apache.parquet.hadoop.example.ExampleParquetWriter;


import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType.file;

/**
 * Parquet Reader
 */
public class MyParquetReader {
    public static void parquetReader() throws IOException {
        ParquetReader.Builder<Group> builder = AvroParquetReader.builder(new GroupReadSupport(), new Path("hdfs://10.1.12.201:8020/user/hive/warehouse/t_parquet/000000_0"));
        ParquetReader<Group> parquetReader = builder.build();

        Group group = null;
        SimpleGroup simpleGroup = null;
        while ((group = parquetReader.read()) != null) {
            //通过名称和下标都可以获取数据数值
//            simpleGroup = (SimpleGroup) group;
//            System.out.println(simpleGroup.getString(0, 0));
//            System.out.println(simpleGroup.getInteger(1,0));
            System.out.println(group.toString());
        }

    }

    //optional，required(非空)，repeated
    //optional、required类似数据库可以为null
    //repeated支持复杂的嵌套结构
    public static MessageType getMessageTypeFromCode() {
        MessageType messageType =
                Types.buildMessage()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("id")
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
                        .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                        .requiredGroup()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test1")
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test2")
                        .named("group1")
                        .named("trigger");
        return messageType;
    }


  /*  public static void parquetWriter(Path file) {
        ParquetWriter.Builder builder = new ParquetWriter.Builder(file).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(getMessageTypeFromCode());


    }*/


    /**
     * java操作写parquet，
     *
     * @param name
     */
    public static void writeParquetCurrent(String name) {

        // 1. 声明parquet的messageType
        MessageType messageType = getMessageTypeFromCode();
        System.out.println(messageType.toString());

        // 2. 声明parquetWriter
        Path path = new Path("d:\\" + name);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType, configuration);
        final GroupWriteSupport writeSupport = new GroupWriteSupport();
        ParquetWriter<Group> writer = null;

//   /*     // 3. 写数据
        //新的API,版本要求是1.9 parquet-hadoop 类似的parquet-column也要对应
        try {
            ExampleParquetWriter.Builder builder = ExampleParquetWriter
                    .builder(path).withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    //.withConf(configuration)
                    .withType(getMessageTypeFromCode());

            writer = builder.build();

            Random random = new Random();

            for (int i = 0; i < 10; i++) {
                // 4. 构建parquet数据，封装成group
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name", i + "@qq.com")
                        .append("id", i + "@id")
                        .append("age", i)
                        .addGroup("group1")
                        .append("test1", "test1" + i)
                        .append("test2", "test2" + i);
                writer.write(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void writeParquetUsed(String name) {

        // 1. 声明parquet的messageType
        MessageType messageType = getMessageTypeFromCode();
        System.out.println(messageType.toString());

        // 2. 声明parquetWriter
        Path path = new Path("E:\\" + name);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType, configuration);
        final GroupWriteSupport writeSupport = new GroupWriteSupport();
        ParquetWriter<Group> writer = null;

        try {

            writer = new ParquetWriter<Group>(path,
                    ParquetFileWriter.Mode.CREATE,
                    writeSupport,
                    CompressionCodecName.UNCOMPRESSED,
                    128*1024*1024,
                    5*1024*1024,
                    5*1024*1024,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetWriter.DEFAULT_WRITER_VERSION,
                    configuration);
            Random random = new Random();

            for (int i = 0; i < 10; i++) {
                // 4. 构建parquet数据，封装成group
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name", i + "@qq.com")
                        .append("id", i + "@id")
                        .append("age", i)
                        .addGroup("group1")
                        .append("test1", "test1" + i)
                        .append("test2", "test2" + i);
                writer.write(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) throws IOException {
//        MyParquetReader.parquetReader();
//        System.out.println(getMessageTypeFromCode().toString());
//        writeParquetCurrent("newparfile");
          writeParquetUsed("deprecatedParquet");
    }
}
