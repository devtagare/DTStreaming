package com.dt.weather.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.constants.WeatherConstants;

public class DefaultConverter extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DefaultConverter.class);

  private static transient List<KeyValPair<String, Integer>> defaultCache = null;
  
  private volatile int sum =0;

  public DefaultConverter()
  {
    defaultCache = new ArrayList<KeyValPair<String, Integer>>();
  }

  public final transient DefaultInputPort<KeyValPair<String, Integer>> data = new DefaultInputPort<KeyValPair<String, Integer>>()
  {
    @Override
    public void process(KeyValPair<String, Integer> tuple)
    {
      //Add the incoming tuples to the list
      //Flush the list when you reach end window
      if (tuple != null) {
        defaultCache.add(tuple);
      }

    }
  };

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void beginWindow(long windowId)
  {
    
    
    defaultCache = new ArrayList<KeyValPair<String,Integer>>();
    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
    ListIterator itr = defaultCache.listIterator();
    
    StringBuilder otuple = new StringBuilder();
    
    otuple.append("[");
    otuple.append("time:"+(System.currentTimeMillis()/1000));
    
    
    while(itr.hasNext()){
      otuple.append(WeatherConstants.RECORD_SEPARATOR);
      KeyValPair<String, Integer> opair = (KeyValPair<String, Integer>)itr.next();
      
      sum +=opair.getValue();
      otuple.append(opair.getKey()+WeatherConstants.TUPLE_SEPARATOR+opair.getValue());
    }
    
    otuple.append(WeatherConstants.RECORD_SEPARATOR);
    otuple.append("Unique Descritptions:"+defaultCache.size());
    
    otuple.append("]");
    
    output.emit(otuple.toString());
    
    defaultCache.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
  }

}
