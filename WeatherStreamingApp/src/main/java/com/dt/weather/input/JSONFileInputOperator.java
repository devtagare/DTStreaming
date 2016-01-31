package com.dt.weather.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileUtil;
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

  private String processedDirPath;

  private String json;

  private volatile boolean found;

  public JSONFileInputOperator()
  {

    json = "";

  }

  public String getProcessedDirPath()
  {
    return processedDirPath;
  }

  public void setProcessedDirPath(String processedDirPath)
  {
    this.processedDirPath = processedDirPath;
  }

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
    Path prevPath = new Path(super.currentFile);

    String str = super.currentFile;

    String[] arr = str.split("/");

    Path processedPath = new Path(getProcessedDirPath());

    super.closeFile(is);
    br.close();

    FileUtil.copy(super.fs, prevPath, super.fs, processedPath, false, super.configuration);

    br = null;
  }

  @Override
  protected void emit(String arg0)
  {
    json += arg0;

    parser = new JSONParser();

    KeyFinder finder = new KeyFinder();

    finder.setMatchKey(getMatchKey());

    try {
      while (!finder.isEnd()) {

        parser.parse(json, finder, true);

        found = true;

        if (finder.isFound()) {
          finder.setFound(false);
          output.emit(new KeyValPair<String, Integer>(finder.getValue().toString(), 1));

        }

      }
    } catch (Exception e) {
   //   LOG.error("Exception in parsing" + e.getLocalizedMessage());
      found = false;

    }

  }

  @Override
  protected String readEntity() throws IOException
  {
    String line = br.readLine();

    if (found) {
      found = false;
      json = "";
    }

    return line;
  }

}
