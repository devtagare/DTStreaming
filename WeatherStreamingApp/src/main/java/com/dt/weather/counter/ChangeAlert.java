package com.dt.weather.counter;

import java.util.HashMap;

import javax.validation.constraints.Min;

import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Operator compares consecutive values arriving at input port mapped by keys,
 * emits &lt;key,percent change&gt; pair on output alert port if percent change
 * exceeds percentage threshold set in operator.
 * <p>
 * StateFull : Yes, current key/value is stored in operator for comparison in
 * next successive windows. <br>
 * Partition(s): No, base comparison value will be inconsistent across
 * instantiated copies. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>alert</b>: emits KeyValPair&lt;K,KeyValPair&lt;V,Double&gt;&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>threshold</b>: The threshold of change between consecutive tuples of the
 * same key that triggers an alert tuple<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * 
 * @displayName Change Alert Key Value
 * @category Rules and Alerts
 * @tags change, key value, numeric, percentage
 * @since 0.3.3
 */
public class ChangeAlert<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
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
      //Emitt the tuple in this case as its unique anyways

      //Add the tuple value to the value in the base map
      if (val == null) {
        val = new MutableInt(tval);

        basemap.put(cloneKey(key), val);
        alert.emit("< Uniq First" + new KeyValPair<K, Integer>(key, tval) + ">");
        return;
      } 
      
      if (tval >= absoluteThreshold) {
        val.setValue(val.intValue() + tval);
        
        //basemap.put(cloneKey(key), val);
        basemap.put(key, val);
        alert.emit("< Uniq Second" + new KeyValPair<K, Integer>(key, tval) + ">");
        return;
      }

    }
  };

  /**
   * Key,Percent Change output port.
   */
  //public final transient DefaultOutputPort<KeyValPair<K, Integer>> alert = new DefaultOutputPort<KeyValPair<K, Integer>>();
  
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
  public double getPercentThreshold()
  {
    return absoluteThreshold;
  }

  /**
   * setter function for threshold value
   */
  public void setPercentThreshold(int d)
  {
    absoluteThreshold = d;
  }

  
}
