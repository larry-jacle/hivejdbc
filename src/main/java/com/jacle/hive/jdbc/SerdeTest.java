package com.jacle.hive.jdbc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Serde 老版本API
 */
public class SerdeTest implements Deserializer {
    private List<String> fieldNames=new ArrayList<String>();
    private List<ObjectInspector>  fieldNameObjectInspectors=new ArrayList<ObjectInspector>();

    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        //添加fieldname和fieldname type
        fieldNames.add("name");
        fieldNameObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        fieldNames.add("age");
        fieldNameObjectInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
       //writable返回的是一行的数据，对这一行的数据进行拆分
        if(writable==null)
        {
            return null;
        }

         if(writable instanceof Text)
         {
             Text contentText=(Text)writable;
             if(contentText==null)
             {
                 return null;
             }else
             {
                 List<Object> list=new ArrayList<Object>();
                 String[] valArr=contentText.toString().split("\\s+");

                 //添加具体的数值
                 list.add(valArr[0]);
                 list.add(Integer.parseInt(valArr[1]));

                 return list;
             }

         }

        return null;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldNameObjectInspectors);
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
