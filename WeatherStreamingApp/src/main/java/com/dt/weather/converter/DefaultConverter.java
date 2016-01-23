package com.dt.weather.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class DefaultConverter extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DefaultConverter.class);
  
  public final transient DefaultInputPort<KeyValPair<Integer,Integer>> data = new DefaultInputPort<KeyValPair<Integer,Integer>>()
      {
        @Override
        public void process(KeyValPair<Integer,Integer> tuple)
        {

          if(tuple!=null){
          
            String str = "<Global," + tuple.getKey() + "," + tuple.getValue() +" >";
          
            output.emit(str);
          }

        }
      };

      public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
}