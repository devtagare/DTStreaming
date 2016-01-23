package com.dt.weather.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyHashValPair;
import com.datatorrent.lib.util.KeyValPair;

public class UniquesConverter extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(UniquesConverter.class);
  
  public final transient DefaultInputPort<KeyHashValPair<KeyValPair<Integer,Integer>,Integer>> data = new DefaultInputPort<KeyHashValPair<KeyValPair<Integer,Integer>,Integer>>()
      {
        @Override
        public void process(KeyHashValPair<KeyValPair<Integer,Integer>,Integer> tuple)
        {
          
          

          if(tuple!=null){
          
            String str = "<Unique," + tuple.getKey().getKey() + "," + tuple.getKey().getValue() +" >";
          
            output.emit(str);
          }

        }
      };

      public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
}
