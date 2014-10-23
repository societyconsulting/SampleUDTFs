package com.spectralclustering.udtf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;


/**
 * Created by dillonlaird on 10/9/14.
 */
public class WebMatTest {
    @Test
    public void testInitialize() throws UDFArgumentException {
        WebMat example = new WebMat();

        ObjectInspector stringIO = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector longIO   = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ObjectInspector intIO    = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

        StructObjectInspector resultInspector = example.initialize(new ObjectInspector[]
                { stringIO, longIO, intIO});

        // don't know what to do with resultInspector
    }

    @Test
    public void testProcess() throws HiveException {
        WebMat example = new WebMat();

        ObjectInspector stringIO = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector longIO   = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        ObjectInspector intIO    = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

        StructObjectInspector resultInspector = example.initialize(new ObjectInspector[]
                { stringIO, longIO, intIO});

        // not sure what objects to pass into process
        // example.process();
    }

    @Test
    public void testSymmetricKeysLinkMapSmall() {
        Map<Pair<Integer, Integer>, Integer> linkMap = new HashMap<Pair<Integer, Integer>, Integer>();

        Pair<Integer, Integer> link1 = new Pair<Integer, Integer>(0, 1);
        Pair<Integer, Integer> link2 = new Pair<Integer, Integer>(1, 0);

        linkMap.put(link1, 0);
        Assert.assertEquals(true, linkMap.containsKey(link2));
    }

    @Test
    public void testSymmetricKeysLinkMapLarge() {
        Map<Pair<Integer, Integer>, Integer> linkMap = new HashMap<Pair<Integer, Integer>, Integer>();

        List<Pair<Integer, Integer>> links = new ArrayList<Pair<Integer, Integer>>();
        links.add(new Pair<Integer, Integer>(0, 1));
        links.add(new Pair<Integer, Integer>(1, 2));
        links.add(new Pair<Integer, Integer>(2, 1));
        links.add(new Pair<Integer, Integer>(2, 1));

        for (Pair<Integer, Integer> link : links) {
            if (!linkMap.containsKey(link))
                linkMap.put(link, 1);
            else
                linkMap.put(link, linkMap.get(link) + 1);
        }

        Assert.assertEquals(true, linkMap.get(new Pair<Integer, Integer>(0, 1)).equals(1));
        Assert.assertEquals(true, linkMap.get(new Pair<Integer, Integer>(1, 2)).equals(3));
    }
}
