package com.dt.weather.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.app.OperatorContextHelper;
import com.dt.weather.event.convertor.WeatherEventConvertor;

public class JSONFileInputOperator extends AbstractFileInputOperator<String>
{

  private static final Logger LOG = LoggerFactory.getLogger(WeatherEventConvertor.class);

  public transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>();

  private static JSONParser parser = new JSONParser();

  BufferedReader br;

  private String matchKey;

  public String getMatchKey()
  {
    return matchKey;
  }

  public void setMatchKey(String matchKey)
  {
    this.matchKey = matchKey;
  }

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

    //Uncomment these when the regex from directory scanner works
    //    Path prevPath = new Path(super.currentFile);
    //    
    //    Path processedPath = new Path(super.currentFile + ".proc");

    super.closeFile(is);
    br.close();

    //    FileContext fc = FileContext.getFileContext(super.fs.getUri());
    //    
    //    fc.rename(prevPath, processedPath, Rename.OVERWRITE);

    br = null;
  }

  @Override
  protected void emit(String arg0)
  {
    parser = new JSONParser();

    KeyFinder finder = new KeyFinder();
    finder.setMatchKey(getMatchKey());
    try {
      while (!finder.isEnd()) {
        parser.parse(arg0, finder, true);
        if (finder.isFound()) {
          finder.setFound(false);

          output.emit(new KeyValPair<String, Integer>(finder.getValue().toString(), 1));
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in parsing"+e.getLocalizedMessage());
    }

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

    JSONFileInputOperator jsonReader = new JSONFileInputOperator();

    jsonReader.setDirectory(dir);

    //    jsonReader.setPartitionCount(1);
    jsonReader.setScanIntervalMillis(0);
    jsonReader.setEmitBatchSize(10);

    //jsonReader.getScanner().setFilePatternRegexp(".txt2");

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
