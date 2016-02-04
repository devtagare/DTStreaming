package com.dt.weather.helper;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.dt.weather.app.OperatorContextHelper;
import com.dt.weather.app.OperatorContextHelper.TestIdOperatorContext;
import com.dt.weather.input.JSONFileInputOperator;

public class JSONFileInputOperatorTest
{
  public static void main(String[] args)
  {
    String dir = "/Users/dev/checkout/personalGit/devel2/DTStreaming/WeatherStreamingApp/src/test/resources/test-data";
    Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(Context.DAGContext.APPLICATION_PATH, dir);
    Context.OperatorContext context = new OperatorContextHelper.TestIdOperatorContext(1, attributes);

    JSONFileInputOperator jsonReader = new JSONFileInputOperator();

    jsonReader.setDirectory(dir);

    jsonReader.setMatchKey("description");

    jsonReader.setScanIntervalMillis(0);
    jsonReader.setEmitBatchSize(10);

    jsonReader.setup(context);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    jsonReader.output.setSink(sink);

    try {
      for (long wid = 0; wid < 100; wid++) {
        jsonReader.beginWindow(wid);

        jsonReader.emitTuples();
        jsonReader.endWindow();
      }
    }

    catch (Exception e) {
      e.printStackTrace();
    }
    jsonReader.teardown();
  }
}
