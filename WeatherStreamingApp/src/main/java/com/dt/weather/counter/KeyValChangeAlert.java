package com.dt.weather.counter;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.validation.constraints.Min;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.constants.WeatherConstants;

public class KeyValChangeAlert<K, V extends Number> extends BaseNumberKeyValueOperator<K, V> implements Operator
{
  /**
   * Base map is a StateFull field. It is retained across windows
   */
  private HashMap<K, MutableInt> basemap = new HashMap<K, MutableInt>();

  private volatile ConcurrentHashMap<K, MutableInt> emitMap = new ConcurrentHashMap<K, MutableInt>();

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
        emitMap.put(cloneKey(key), val);

        //  alert.emit(new KeyValPair<K, Integer>(key, tval));
        return;
      } else if (tval >= getAbsoluteThreshold()) {
        val.setValue(val.intValue() + tval);
        basemap.put(cloneKey(key), val);
        emitMap.put(key, val);

        //alert.emit(new KeyValPair<K, Integer>(key, val.intValue()));
        return;
      }

    }
  };

  // public final transient DefaultOutputPort<KeyValPair<K, Integer>> alert = new DefaultOutputPort<KeyValPair<K, Integer>>();

  public final transient DefaultOutputPort<String> alert = new DefaultOutputPort<String>();

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

  public void endWindow()
  {

    StringBuilder outTuple = new StringBuilder();

    outTuple.append("<time: " + System.currentTimeMillis() / 1000);

    for (Entry<K, MutableInt> entry : emitMap.entrySet()) {
      String key = (String)entry.getKey();
      MutableInt value = entry.getValue();

      outTuple.append(WeatherConstants.RECORD_SEPARATOR);
      outTuple.append(key + WeatherConstants.TUPLE_SEPARATOR + value);
      emitMap.remove(key);

    }

    outTuple.append(WeatherConstants.RECORD_SEPARATOR);
    outTuple.append("Uniques :" + basemap.size());

    outTuple.append(" >");

    alert.emit(outTuple.toString());

    outTuple = null; 
    
   // emitMap.clear();

  }

}
