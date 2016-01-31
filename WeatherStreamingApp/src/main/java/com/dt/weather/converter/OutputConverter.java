package com.dt.weather.converter;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.constants.WeatherConstants;

public class OutputConverter<K, V extends Number> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(OutputConverter.class);

  public final transient DefaultInputPort<HashMap<K, MutableDouble>> data = new DefaultInputPort<HashMap<K, MutableDouble>>()
  {
    @Override
    public void process(HashMap<K, MutableDouble> tuple)
    {

      emitChangedAggregates(tuple, "Uniques");

    }
  };

  public final transient DefaultInputPort<HashMap<K, MutableDouble>> dataCumulative = new DefaultInputPort<HashMap<K, MutableDouble>>()
  {
    @Override
    public void process(HashMap<K, MutableDouble> tuple)
    {

      emitChangedAggregates(tuple, "Total Counts");

    }
  };

  public void emitChangedAggregates(HashMap<K, MutableDouble> tuple, String str)
  {
    StringBuilder outTuple = new StringBuilder();

    outTuple.append("<time: " + System.currentTimeMillis() / 1000);

    if (tuple.entrySet().size() == 1 && str.equals("Uniques")) {
      return;
    }

    outTuple.append(WeatherConstants.RECORD_SEPARATOR);

    outTuple.append(str);
    

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
}
