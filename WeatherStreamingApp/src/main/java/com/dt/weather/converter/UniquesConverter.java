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

  public final transient DefaultInputPort<KeyHashValPair<KeyValPair<String, Integer>, Integer>> data = new DefaultInputPort<KeyHashValPair<KeyValPair<String, Integer>, Integer>>()
  {
    @Override
    public void process(KeyHashValPair<KeyValPair<String, Integer>, Integer> tuple)
    {

      if (tuple != null) {
        output.emit(new KeyValPair<String, Integer>(tuple.getKey().getKey(), tuple.getKey().getValue()));
      }

    }
  };

  public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>();
}
