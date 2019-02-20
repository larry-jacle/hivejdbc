package com.jacle.hive.jdbc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MySerde extends AbstractSerDe {
    // params
    private List<String> columnNames = null;
    private List<TypeInfo> columnTypes = null;
    private ObjectInspector objectInspector = null;
    // seperator
    private String nullString = null;
    private String lineSep = null;
    private String kvSep = null;


    @Override
    public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
// Read sep
        lineSep = "\n";
        kvSep = "=";
        nullString = tbl.getProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "");

        //下面的都是固定的写法，在新的API中，列的name和type都是直接通过读取表定义的properties来进行赋值的
        // Read Column Names
        String columnNameProp = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        if (columnNameProp != null && columnNameProp.length() > 0) {
            columnNames = Arrays.asList(columnNameProp.split(","));
        } else {
            columnNames = new ArrayList<String>();
        }

        // Read Column Types
        String columnTypeProp = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        // default all string
        if (columnTypeProp == null) {
            String[] types = new String[columnNames.size()];
            Arrays.fill(types, 0, types.length, serdeConstants.STRING_TYPE_NAME);
            columnTypeProp = StringUtils.join(types, ":");
        }
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProp);

        // Check column and types equals
        if (columnTypes.size() != columnNames.size()) {
            throw new SerDeException("len(columnNames) != len(columntTypes)");
        }

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
        ObjectInspector oi;
        for (int c = 0; c < columnNames.size(); c++) {
            oi = TypeInfoUtils
                    .getStandardJavaObjectInspectorFromTypeInfo(columnTypes
                            .get(c));
            columnOIs.add(oi);
        }
        objectInspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnOIs);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        //对一行数据的解析
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
        return objectInspector;
    }
}
