package com.atgenomix.seqslab.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class RetrieveRsID extends GenericUDF {

    private ListObjectInspector ids = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) throw new UDFArgumentException("inputs must have length 1");

        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.LIST))
            throw new UDFArgumentException("input must be a list.");

        ids = (ListObjectInspector) objectInspectors[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects == null || deferredObjects.length != 1) {
            throw new HiveException("received wrong arguments");
        }

        if (deferredObjects[0] ==null || deferredObjects[0].get() == null) {
            return null;
        } else if (this.ids.getList(deferredObjects[0].get()) == null) {
            return null;
        } else {
            ArrayList<Object> outIds = new ArrayList<>();
            ArrayList<Object> ids = new ArrayList<>(this.ids.getList(deferredObjects[0].get()));
            for (Object id: ids){
                if (id.toString().startsWith("rs"))
                    outIds.add(id);
            }
            return outIds;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "retrieve_rs_id";
    }
}
