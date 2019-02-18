package com.jacle.csv;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class CsvUtil {
    public static void readCsv(String path) {
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
//            CSVReader csvReader = new CSVReader(new InputStreamReader(in,"GBK"));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, "GBK"), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            String[] strs;
            while ((strs = csvReader.readNext()) != null) {
                System.out.println("row:"+Arrays.deepToString(strs));
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeFile(String path, String[] strArr, List<String[]> list) {
        File csv = new File(path);
        if (!csv.exists()) {
            try {
                csv.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csv), "GBK"),
                    CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER);
            writer.writeNext(strArr);
            writer.writeAll(list);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
