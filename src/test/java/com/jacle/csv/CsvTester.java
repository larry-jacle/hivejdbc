package com.jacle.csv;

import org.junit.Test;

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
}
