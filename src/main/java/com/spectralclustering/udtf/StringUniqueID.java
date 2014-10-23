package com.spectralclustering.udtf;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


/**
 * Created by dillonlaird on 10/9/14.
 *
 * StringUniqueID takes in a table of strings and outputs a table containing
 * each string and a unqiue ID associated with that string. Here the unique IDs
 * are indices instead of completely random numbers. As an example our input
 * could be
 *
 * www.microsoft.com
 * www.google.com
 * www.microsoft.com
 * www.bing.com
 * www.microsoft.com
 *
 * and our output would be
 *
 * www.microsoft.com 0
 * www.bing.com      1
 * www.google.com    2
 *
 */
public class StringUniqueID extends GenericUDTF {
    // A map of url's to unique ID's
    Map<String, Integer> uniqueIDMap = new HashMap<String, Integer>();

    StringObjectInspector stringIO;

    // unique ID for current url
    int uniqueID = 0;

    @Override
    public void close() throws HiveException {
        for (String str : uniqueIDMap.keySet()) {
            forward(new Object[] { str, uniqueIDMap.get(str) });
        }
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argIOs)
            throws UDFArgumentException {

        if (argIOs.getAllStructFieldRefs().size() < 1)
            throw new UDFArgumentLengthException("StringUniqueID takes in 1 string argument: STRING, ...");

        ObjectInspector a = argIOs.getAllStructFieldRefs().get(0).getFieldObjectInspector();

        if (!(a instanceof StringObjectInspector))
            throw new UDFArgumentException("StringUniqueID takes in 1 string argument: STRING, ...");

        this.stringIO = (StringObjectInspector) a;

        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldIOs = new ArrayList<ObjectInspector>();

        fieldNames.add("col1");
        fieldNames.add("col2");
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldIOs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String str = stringIO.getPrimitiveJavaObject(args[0]);

        if (!uniqueIDMap.containsKey(str))
            uniqueIDMap.put(str, uniqueID++);
    }
}
