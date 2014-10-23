package com.spectralclustering.udtf;

import java.util.*;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


/**
 * Created by dillonlaird on 10/8/14.
 *
 * WebMatUDTF takes a table containing session_id, unix_timestamp, url_id and
 * outputs url_id_1, url_id_2, count where url_id_1 and url_id_2 are two url's
 * that occurred next to each other (in terms of time) in a session and count is
 * the number of times this occurred. The output represents a Matrix where
 * url_id is an index in the matrix. For example our input could be
 *
 * 12345,1000,0
 * 12345,1002,2
 * 12345,1001,1
 * 23456,1000,2
 * 23456,1001,1
 * 34567,1000,0
 * 23456,1002,2
 * 23456,1003,1
 *
 * and the output would be
 *
 * 0,1,1
 * 1,2,4
 *
 * since we have 1 link from 0 to 1 and 4 links from 1 to 2 or visa versa.
 *
 */
public class WebMat extends GenericUDTF {
    // A map from the session ID to a list of url ID, unix timestamp pairs.
    Map<String, List<Pair<Integer, Long>>> sessionMap = new HashMap<String, List<Pair<Integer, Long>>>();

    // A map from pairs of url ID's that represents links to a count of how
    // many times that link appears in the dataset.
    Map<Pair<Integer, Integer>, Integer> linkMap = new HashMap<Pair<Integer, Integer>, Integer>();

    StringObjectInspector stringIO;
    LongObjectInspector longIO;
    IntObjectInspector intIO;

    @Override
    public void close() throws HiveException {
        // Construct linkMap. linkMap holds website links in the key and the
        // number of times they were linked in the value.
        for (String session : sessionMap.keySet()) {
            List<Pair<Integer, Long>> events = sessionMap.get(session);
            Collections.sort(events, new EventComparator());

            if (events.size() >= 2) {
                for (int i = 1; i < events.size(); ++i) {
                    Pair<Integer, Integer> link = new Pair<Integer, Integer>(
                            events.get(i - 1).getFirst(), events.get(i).getFirst());

                    if (!linkMap.containsKey(link))
                        linkMap.put(link, 1);
                    else
                        linkMap.put(link, linkMap.get(link) + 1);
                }
            }
        }

        // For mahout, the matrix is assumed to be symmetric so we only have
        // to output values in the lower or upper triangle. Also, unspecified
        // entries are assumed to be 0.
        for (Pair<Integer, Integer> link : linkMap.keySet())
            forward(new Object[] { link.getFirst(), link.getSecond(), linkMap.get(link) });
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argIOs)
            throws UDFArgumentException {

        // check we received correct objects
        if (argIOs.length != 3)
            throw new UDFArgumentLengthException("WebMatUDTF takes 3 arguments: STRING, BIGINT, STRING");

        ObjectInspector a = argIOs[0];
        ObjectInspector b = argIOs[1];
        ObjectInspector c = argIOs[2];

        if (!(a instanceof StringObjectInspector) || !(b instanceof LongObjectInspector)
                || !(c instanceof IntObjectInspector)) {

            throw new UDFArgumentException("WebMatUDTF takes 3 arguments: STRING, BIGINT, STRING");
        }

        // not sure here, but people seem to instantiate the objects this way
        this.stringIO = (StringObjectInspector) a;
        this.longIO   = (LongObjectInspector)   b;
        this.intIO    = (IntObjectInspector)    c;

        // set up the Inspector
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldIOs = new ArrayList<ObjectInspector>();

        fieldNames.add("col1"); // url link 1 ID
        fieldNames.add("col2"); // ulr link 2 ID
        fieldNames.add("col3"); // count

        fieldIOs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldIOs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String session = (String) stringIO.getPrimitiveJavaObject(args[0]);
        Long unixTimestamp = (Long) longIO.getPrimitiveJavaObject(args[1]);
        Integer url = (Integer) intIO.getPrimitiveJavaObject(args[2]);

        if (!sessionMap.containsKey(session))
            sessionMap.put(session, new ArrayList<Pair<Integer, Long>>());
        sessionMap.get(session).add(new Pair<Integer, Long>(url, unixTimestamp));
    }

    /**
     * This class is for comparing events that are stored in Pair's where each
     * Pair holds a url and a unix timestamp. This class compares the events by
     * their unix timestamps.
     */
    class EventComparator implements Comparator<Pair<Integer, Long>> {
        @Override
        public int compare(Pair<Integer, Long> first, Pair<Integer, Long> second) {
            return first.getSecond().compareTo(second.getSecond());
        }
    }
}
