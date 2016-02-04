package com.dt.weather.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.event.convertor.WeatherEventConvertor;

public class JSONFileInputOperator extends AbstractFileInputOperator<String> implements CheckpointListener
{

  private static final Logger LOG = LoggerFactory.getLogger(WeatherEventConvertor.class);

  public transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>();

  private static JSONParser parser = new JSONParser();

  private ConcurrentHashMap<Long, Set<String>> processedFilesList;

  private String processedDirPath;

  private String json;

  private volatile boolean found;

  public JSONFileInputOperator()
  {

    json = "";

    processedFilesList = new ConcurrentHashMap<Long, Set<String>>();

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

    Long currentWindow = super.currentWindowId;
    String fileName = super.currentFile;

    if (processedFilesList.contains(currentWindow)) {
      processedFilesList.get(currentWindow).add(fileName);

    } else {
      Set<String> tmpSet = new HashSet<String>();
      tmpSet.add(fileName);
      processedFilesList.put(currentWindow, tmpSet);
    }

    super.closeFile(is);
    br.close();
    br = null;
  }

  public void renameFile(String src, String dest)
  {
    Path prevPath = new Path(src);

    Path processedPath = new Path(dest);

    try {
      FileUtil.copy(super.fs, prevPath, super.fs, processedPath, false, super.configuration);
    } catch (IOException e) {
      e.printStackTrace();
    }

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

  public void committed(long windowId)
  {

    Set<String> processedFiles = super.processedFiles;

    for (Entry<Long, Set<String>> entry : processedFilesList.entrySet()) {

      Long completedWindowId = (Long)entry.getKey();

      if (completedWindowId > windowId)
        continue;

      Set<String> filesProcessed = entry.getValue();

      Iterator<String> itr = filesProcessed.iterator();

      while (itr.hasNext()) {
        String fname = itr.next().toString();
        if (processedFiles.contains(fname)) {

          renameFile(fname, getProcessedDirPath());
        }
      }

    }

    //Prune the file list for the files  that are 60 windows behind committed window
    pruneFileList(windowId - 120);

  }

  public void pruneFileList(long windowId)
  {
    for (Entry<Long, Set<String>> entry : processedFilesList.entrySet()) {
      if (entry.getKey().longValue() <= windowId) {
        processedFilesList.remove(entry.getKey());
      }
    }
  }

}
