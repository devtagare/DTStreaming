package com.dt.weather.converter;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.dt.weather.constants.WeatherConstants;

public class OutputConverter<K, V extends Number> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(OutputConverter.class);

  private ConcurrentHashMap<K, MutableDouble> emitMap = new ConcurrentHashMap<K, MutableDouble>();

  private volatile double sum = 0;

  public final transient DefaultInputPort<HashMap<K, MutableDouble>> data = new DefaultInputPort<HashMap<K, MutableDouble>>()
  {
    @Override
    public void process(HashMap<K, MutableDouble> tuple)
    {

      addToMap(tuple);

    }
  };

  public final transient DefaultInputPort<HashMap<K, MutableDouble>> dataCumulative = new DefaultInputPort<HashMap<K, MutableDouble>>()
  {
    @Override
    public void process(HashMap<K, MutableDouble> tuple)
    {

      addToMap(tuple);

    }
  };

  public void addToMap(HashMap<K, MutableDouble> tuple)
  {
    for (Entry<K, MutableDouble> entry : tuple.entrySet()) {

      String key = (String)entry.getKey();
      MutableDouble value = new MutableDouble(entry.getValue());

      if (key.equals("Total")) {
        if (sum < value.doubleValue()) {
          emitMap.put((K)"Total", value);
          sum = value.doubleValue();
        } else {
          emitMap.put((K)key, new MutableDouble(sum));
        }
      } else {
        emitMap.put((K)key, value);
      }

    }

  }

  public void emitChangedAggregates(ConcurrentHashMap<K, MutableDouble> tuple)
  {
    StringBuilder outTuple = new StringBuilder();

    if (tuple.size() < 1) {
      return;
    }
    
    if(emitMap.size()==1 && emitMap.containsKey("Total")){
      return;
    }

    outTuple.append("<time: " + System.currentTimeMillis() / 1000);

    for (Entry<K, MutableDouble> entry : tuple.entrySet()) {

      String key = (String)entry.getKey();
      MutableDouble value = new MutableDouble(entry.getValue());

      outTuple.append(WeatherConstants.RECORD_SEPARATOR);
      outTuple.append(key + WeatherConstants.TUPLE_SEPARATOR + value);

    }

    outTuple.append(" >");

    output.emit(outTuple.toString());

    outTuple = null;
  }

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void endWindow()
  {
    emitChangedAggregates(emitMap);
    emitMap.clear();

  }
}
