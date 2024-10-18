package com.atgenomix.seqslab.udf.hive;

import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;

public class RetrievePattern extends GenericUDF {

    private StringObjectInspector pattern = null;

    private ListObjectInspector samples = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) throw new UDFArgumentException("inputs must have length 2");

        if (!(objectInspectors[0] instanceof StringObjectInspector))
            throw new UDFArgumentException("input genotypes must be a string.");
        if (!objectInspectors[1].getCategory().equals(ObjectInspector.Category.LIST))
            throw new UDFArgumentException("input removeIds must be a list.");

        pattern = (StringObjectInspector) objectInspectors[0];
        samples = (ListObjectInspector) objectInspectors[1];

        if (!(samples.getListElementObjectInspector() instanceof StringObjectInspector))
            throw new UDFArgumentException("input genotypes must be a list of string");

        return samples;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects == null || deferredObjects.length != 2) {
            throw new HiveException("received wrong arguments");
        }
        Pattern pattern = Pattern.compile(this.pattern.getPrimitiveJavaObject(deferredObjects[0].get()));
        if (this.samples.getList(deferredObjects[1].get()) == null) {
            return null;
        } else {
            ArrayList<Object> samples = new ArrayList<>(this.samples.getList(deferredObjects[1].get()));
            ArrayList<String> outputSamples = new ArrayList<>();
            for (Object sample : samples) {
                String sampleS = sample.toString();
                if (pattern.matcher(sampleS).find()) {
                    outputSamples.add(sampleS);
                }
            }
            return outputSamples;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "retrieve_pattern";
    }
}
