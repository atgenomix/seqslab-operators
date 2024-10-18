package com.atgenomix.seqslab.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;

public class ExceptSampleId extends GenericUDF {

    private ListObjectInspector genotypes = null;
    private StructObjectInspector genotype = null;
    private ListObjectInspector removeIds = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) throw new UDFArgumentException("inputs must have length 2");

        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.LIST))
            throw new UDFArgumentException("input genotypes must be a list.");
        if (!objectInspectors[1].getCategory().equals(ObjectInspector.Category.LIST))
            throw new UDFArgumentException("input removeIds must be a list.");

        genotypes = (ListObjectInspector) objectInspectors[0];
        removeIds = (ListObjectInspector) objectInspectors[1];

        if (genotypes.getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT)
            throw new UDFArgumentException("input genotypes must be a list of struct");

        genotype = (StructObjectInspector) genotypes.getListElementObjectInspector();
        return genotypes;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects == null || deferredObjects.length != 2) {
            throw new HiveException("received wrong arguments");
        }

        ArrayList<Object> genotypes = new ArrayList<>(this.genotypes.getList(deferredObjects[0].get()));
        ArrayList<Object> removeIds = new ArrayList<>(this.removeIds.getList(deferredObjects[1].get()));
        StructField field = genotype.getStructFieldRef("sampleId");
        ArrayList<Object> outGenotypes = new ArrayList<>();

        for (Object genotype : genotypes) {
            Object sampleId = this.genotype.getStructFieldData(genotype, field);
            if (!removeIds.contains(sampleId.toString()))
                outGenotypes.add(genotype);
        }
        return outGenotypes;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "except_sample_id";
    }
}
