package com.dt.weather.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.dt.weather.app.OperatorContextHelper;

public class SimpleFileReader extends AbstractFileInputOperator<String>
{
  
 public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
 
 BufferedReader br;

 
 @Override
 protected InputStream openFile(Path path) throws IOException
 {
   InputStream is = super.openFile(path);
   br = new BufferedReader(new InputStreamReader(is));
   return is;
 }

 @Override
 protected void closeFile(InputStream is) throws IOException
 {
   super.closeFile(is);
   br.close();
   //TODO - rename a file once its done reading
   br = null;
 }

  @Override
  protected void emit(String arg0)
  {
    System.out.println("Here");
    System.out.println("Event:"+arg0);
   output.emit(arg0.toString());
    
  }

  @Override
  protected String readEntity() throws IOException
  {
    return br.readLine();
    
  }
  
  public static void main(String[] args)
  {
    String dir = "/Users/dev/Desktop/test";
    Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(Context.DAGContext.APPLICATION_PATH, dir);
    Context.OperatorContext context = new OperatorContextHelper.TestIdOperatorContext(1, attributes);
    
    SimpleFileReader jsonReader = new SimpleFileReader();
    
    jsonReader.setDirectory(dir);
    
//    jsonReader.setPartitionCount(1);
    jsonReader.setScanIntervalMillis(0);
    jsonReader.setEmitBatchSize(10);
    
    //jsonReader.getScanner().setFilePatternRegexp(".txt2");
    
    jsonReader.setup(context);
    
    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    jsonReader.output.setSink(sink);

   

   try
   { for (long wid=0; wid<10; wid++) {
      jsonReader.beginWindow(wid);
     
      jsonReader.emitTuples();
      jsonReader.endWindow();
    }
  }
   
   catch(Exception e){
     e.printStackTrace();
   }
    jsonReader.teardown();
  }

}
