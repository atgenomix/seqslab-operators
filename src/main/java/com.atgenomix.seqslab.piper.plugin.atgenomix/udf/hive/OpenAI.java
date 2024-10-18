package com.atgenomix.seqslab.piper.plugin.atgenomix.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hc.client5.http.ClientProtocolException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


public class OpenAI extends GenericUDF {

    private String API_URL = "https://seqslab-openai-eastus.openai.azure.com/openai/deployments/seqslab/chat/completions";
    private String API_KEY = "1960df1142724adabf112d2b7811a183";
    private String API_VERSION = "2024-02-01";
    private String DEPLOYMENT_OR_MODEL_NAME = "seqslab";
    private StringObjectInspector system = null;
    private ListObjectInspector demonstration = null;
    private StringObjectInspector prompt = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3) {
            throw new UDFArgumentException("inputs must have length 3");
        }

        if (!(objectInspectors[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first input must be a String.");
        }

        if (objectInspectors[1].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("second input must be a List.");
        }

        ObjectInspector element = ((ListObjectInspector)objectInspectors[1]).getListElementObjectInspector();
        boolean nestedArray = element instanceof ListObjectInspector;
        if (nestedArray && !(((ListObjectInspector)element).getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("second input must be a List[List[String]].");
        }

        if (!(objectInspectors[2] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("third input must be a String.");
        }

        system = (StringObjectInspector) objectInspectors[0];
        demonstration = (ListObjectInspector) objectInspectors[1];
        prompt = (StringObjectInspector) objectInspectors[2];

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects == null || deferredObjects.length != 3) {
            throw new HiveException("received wrong arguments");
        }

        if (deferredObjects[0] == null || deferredObjects[0].get() == null) {
            throw new HiveException("first input should not be null");
        }

        if (deferredObjects[1] == null || deferredObjects[1].get() == null) {
            throw new HiveException("second input should not be null");
        }

        if (deferredObjects[2] == null || deferredObjects[2].get() == null) {
            throw new HiveException("third input should not be null");
        }

        String sym = this.system.getPrimitiveJavaObject(deferredObjects[0].get());
        List demo = this.demonstration.getList(deferredObjects[1].get());
        String p = this.prompt.getPrimitiveJavaObject(deferredObjects[2].get());

        String answer = "";
        if (demo.isEmpty()) {
            answer = zeroShotQuery(sym, demo, p);
        } else if (demo.get(0) instanceof String) {
            answer = zeroShotQuery(sym, demo, p);
        } else {
            answer = fewShotQuery(sym, demo, p);
        }

        return answer;
    }

    private String zeroShotQuery(String system, List<String> demonstrations, String prompt) {
        StringBuilder builder = new StringBuilder();
        for (String demo: demonstrations) {
            builder.append(demo).append("\n");
        }

        List<String> contents = new ArrayList<>();
        if (builder.length() > 0) {
            contents.add(builder.toString());
        }

        String result = query(system, contents, prompt);
        return result;
    }

    private String fewShotQuery(String system, List<List<String>> demonstrations, String prompt) {
        List<String> contents = new ArrayList<>();
        for (List<String> demo: demonstrations) {
            assert(demo.size() ==2);
            String msg = String.format("Question: %s\nAnswer: %s.", demo.get(0), demo.get(1));
            contents.add(msg);
        }

        String result = query(system, contents, prompt);
        return result;
    }

    private String query(String system, List<String> contents, String prompt) {
        List<JSONObject> messages = new ArrayList<>();
        JSONObject systemMessage = new JSONObject();
        systemMessage.put("role", "system");
        systemMessage.put("content", system);
        messages.add(systemMessage);
        JSONObject assistentMessage = new JSONObject();
        assistentMessage.put("role", "assistant");
        assistentMessage.put("content", "You are an AI assistant that helps people find information using Traditional Chinese.");
        messages.add(assistentMessage);

        for (String c: contents) {
            JSONObject userMessage = new JSONObject();
            userMessage.put("role", "user");
            userMessage.put("content", c);
            messages.add(userMessage);
        }

        JSONObject userMessage = new JSONObject();
        userMessage.put("role", "user");
        userMessage.put("content", "#zh-tw " + prompt);
        messages.add(userMessage);

        JSONObject requestBody = new JSONObject();
        requestBody.put("messages", new JSONArray(messages));
        requestBody.put("temperature", 0.0);
        requestBody.put("max_tokens", 800);
        requestBody.put("top_p", 0.95);
        requestBody.put("frequency_penalty", 0);
        requestBody.put("presence_penalty", 0);
        requestBody.put("stop", JSONObject.NULL);

        String response = "";
        CloseableHttpClient httpClient;
        try {
            httpClient = HttpClients.createDefault();
            HttpPost request = new HttpPost(API_URL);
            URI uri = new URIBuilder(request.getUri()).addParameter("api-version", API_VERSION)
                    .build();
            request.setUri(uri);
            request.setHeader("Content-Type", "application/json");
            request.setHeader("api-key", API_KEY);

            StringEntity params = new StringEntity(requestBody.toString());
            request.setEntity(params);

            response = httpClient.execute(request, httpResponse -> {
                int status = httpResponse.getCode();
                String respMsg = "";

                if (status >= HttpStatus.SC_SUCCESS && status < HttpStatus.SC_REDIRECTION) {
                    final HttpEntity entity = httpResponse.getEntity();
                    try {
                        if (entity != null) {
                            String responseBody = EntityUtils.toString(entity);
                            JSONObject jsonResponse = new JSONObject(responseBody);
                            JSONArray jsonArray = jsonResponse.getJSONArray("choices");
                            JSONObject firstElement = jsonArray.getJSONObject(0);
                            JSONObject message = firstElement.getJSONObject("message");

                            respMsg = message.getString("content");
                        }
                    } catch (final ParseException ex) {
                        throw new ClientProtocolException(ex);
                    }
                } else {
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }

                return respMsg;
            });

            httpClient.close();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

        return response;
    }


    @Override
    public String getDisplayString(String[] strings) {
        return "gpt";
    }
}
