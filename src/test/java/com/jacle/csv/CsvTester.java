package com.jacle.csv;

import org.junit.Test;

import java.util.StringTokenizer;

public class CsvTester
{
    @Test
    public void test()
    {
        CsvUtil.readCsv("d:/hive_csv.csv");
        int i=0;
        int m=i++;
        System.out.println(m);
        System.out.println(i);

    }


    @Test
    public void testStringTokenizer()
    {
        //默认分隔符：” \t\n\r\f”（前有一个空格，引号不是）
        String str="www.baidu.com";
        StringTokenizer tokenizer=new StringTokenizer(str,".b");

        while(tokenizer.hasMoreElements())
        {
           System.out.println(tokenizer.nextToken());
        }

        String s="123 sdfdsf    sdf";
        String[] arr=s.split("\\s+");

        for(String item:arr)
        {
            System.out.println(item);
        }
    }
}
