package com.dt.weather.event.convertor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.tools.ant.types.CommandlineJava.SysProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.constants.WeatherConstants;
import com.dt.weather.model.WeatherEvent;

public class SinglePortWeatherEventConvertor implements Operator
{

  private static final Logger LOG = LoggerFactory.getLogger(WeatherEventConvertor.class);

  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {

      List<WeatherEvent> events = new LinkedList<WeatherEvent>();
      try {

        events = parseEvent(tuple);

      } catch (Exception e) {
        LOG.error("Exception in parsing the incoming tuple" + e.getLocalizedMessage());
      }

      ListIterator<WeatherEvent> itr = events.listIterator();

      while (itr.hasNext()) {
        WeatherEvent event = itr.next();
        if (event != null) {

          KeyValPair<String, Integer> pair = new KeyValPair<String, Integer>(event.getDescription(), event.getCount());
          output.emit(pair);

        }
      }

    }
  };

  public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>();

  private List<WeatherEvent> parseEvent(String tuple)
  {

    List<WeatherEvent> eventList = new LinkedList<WeatherEvent>();

    WeatherEvent event = null;

    String cityName = "";
    long ts = 0L;

    org.json.JSONObject line = null;
    try {
      line = new org.json.JSONObject(tuple);

      if (line.has(WeatherConstants.CITY)) {
        JSONObject city = line.getJSONObject(WeatherConstants.CITY);

        if (city.has(WeatherConstants.NAME)) {

          cityName = city.getString(WeatherConstants.NAME);

        }

      }

      if (line.has(WeatherConstants.DATA_ARR)) {
        JSONArray dataArr = line.getJSONArray(WeatherConstants.DATA_ARR);

        for (int i = 0; i < dataArr.length(); i++) {
          JSONObject dataObj = new JSONObject(dataArr.get(i).toString());

          if (dataObj.has(WeatherConstants.WEATHER)) {
            JSONArray weatherObj = dataObj.getJSONArray(WeatherConstants.WEATHER);
            JSONObject weatherID = weatherObj.getJSONObject(0);

            event = new WeatherEvent();

            if (weatherID != null) {
              event.setWeatherID(weatherID.getInt(WeatherConstants.ID));
              event.setDescription(weatherID.getString(WeatherConstants.WEATHER_DESC));
            }

            event.setCity(cityName);

            event.setCount(1);

            eventList.add(event);

            event = null;
          }

        }
      }

    } catch (JSONException e) {

      LOG.error("Exception in parsing JSON event :" + e.getLocalizedMessage());
      return null;
    }

    System.out.println("EventList is:" + eventList.toString());
    return eventList;
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

  public static void main(String[] args)
  {

    File file = new File(
        "/Users/dev/checkout/personalGit/devel/DTStreaming/WeatherStreamingApp/src/test/resources/data/WeatherInfoTest.json");
    FileInputStream fis = null;
    BufferedReader br = null;

    SinglePortWeatherEventConvertor event = new SinglePortWeatherEventConvertor();

    try {
      fis = new FileInputStream(file);

      br = new BufferedReader(new InputStreamReader(fis));

      String line = null;

      while ((line = br.readLine()) != null) {
        event.data.process(line);
      }

    } catch (Exception e) {

    }
  }

}
