package com.jacle;

import com.jacle.hive.jdbc.CreateOrcFile;
import com.jacle.mr.MrBase;
import com.jacle.mr.WordCount;
import com.jacle.parquet.MRParquet;
import org.apache.hadoop.util.ProgramDriver;

public class RunDriverClass
{
    public static void main(String[] args)
    {
        ProgramDriver programDriver=new ProgramDriver();
        int exitCode=0;
        try {
            programDriver.addClass("selfwordcount", WordCount.class,"selfwordcount");
            programDriver.addClass("mrbase", MrBase.class,"mrbase");
            programDriver.addClass("createorcfile", CreateOrcFile.class,"createorcfile");
            programDriver.addClass("parquet", MRParquet.class,"mrParquet");

            exitCode=programDriver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
