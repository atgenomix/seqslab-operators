package com.atgenomix.seqslab.piper.plugin.atgenomix.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.*;

public class RetrievePositiveGenotype extends GenericUDF {

    private ListObjectInspector genotypes = null;
    private StructObjectInspector genotype = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) throw new UDFArgumentException("inputs must have length 1");

        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.LIST))
            throw new UDFArgumentException("input genotypes must be a list.");

        genotypes = (ListObjectInspector) objectInspectors[0];

        if (genotypes.getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT)
            throw new UDFArgumentException("input genotypes must be a list of struct");

        genotype = (StructObjectInspector) genotypes.getListElementObjectInspector();
        return genotypes;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects == null || deferredObjects.length != 1) {
            throw new HiveException("received wrong arguments");
        }

        ArrayList<Object> outGenotypes = new ArrayList<>();
        ArrayList<Object> genotypes = new ArrayList<>(this.genotypes.getList(deferredObjects[0].get()));
        StructField callsF = genotype.getStructFieldRef("calls");
        List<Integer> call1 = Arrays.asList(0, 1);
        List<Integer> call2 = Arrays.asList(1, 1);
        List<Integer> call3 = Collections.singletonList(1);

        for (Object genotype : genotypes) {
            Object calls = this.genotype.getStructFieldData(genotype, callsF);
            List<?> list = new ArrayList<>();
            if (calls.getClass().isArray()) {
                list = Arrays.asList((Object[])calls);
            } else if (calls instanceof Collection) {
                list = new ArrayList<>((Collection<?>)calls);
            }
            if (list.equals(call1) || list.equals(call2) || list.equals(call3))
                outGenotypes.add(genotype);
        }
        return outGenotypes;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "retrieve_positive_genotype";
    }
}
