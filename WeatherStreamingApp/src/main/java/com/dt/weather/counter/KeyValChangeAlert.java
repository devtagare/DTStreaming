package com.dt.weather.counter;

import java.util.HashMap;

import javax.validation.constraints.Min;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;

public class KeyValChangeAlert<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * Base map is a StateFull field. It is retained across windows
   */
  private HashMap<K, MutableInt> basemap = new HashMap<K, MutableInt>();

  /**
   * Input data port that takes a key value pair.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * Process each key, compute change or percent, and emit it.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      int tval = tuple.getValue().intValue();
      MutableInt val = basemap.get(key);

      //Insert into the base map if its not present
      //Emit the tuple in this case as its unique anyways

      //Add the tuple value to the value in the base map

      if (val == null) {
        val = new MutableInt(tval);
        basemap.put(cloneKey(key), val);
        alert.emit(new KeyValPair<K, Integer>(key, tval));
        return;
      } 
      
      if (tval >= absoluteThreshold) {
        val.setValue(val.intValue() + tval);
        basemap.put(key, val);
        alert.emit(new KeyValPair<K, Integer>(key, val.intValue()));
        return;
      }

    }
  };

  public final transient DefaultOutputPort<KeyValPair<K, Integer>> alert = new DefaultOutputPort<KeyValPair<K, Integer>>();

  /**
   * Alert threshold hold percentage set by application.
   */
  @Min(1)
  private int absoluteThreshold = 0;

  /**
   * getter function for threshold value
   *
   * @return threshold value
   */
  @Min(1)
  public double getAbsoluteThreshold()
  {
    return absoluteThreshold;
  }

  /**
   * setter function for threshold value
   */
  public void setAbsoluteThreshold(int d)
  {
    absoluteThreshold = d;
  }

}
