package com.dt.weather.counter;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;

import java.util.HashMap;
import java.util.Map;

/**
 * This unifier consumes hash maps whose keys are objects and whose values are numbers.&nbsp;
 * The values for each key are summed and emitted in a hash map at the end of each application window.
 * <p>
 * This unifier uses round robin partitioning.
 * </p>
 * @displayName Unifier Hash Map Sum Keys
 * @category Algorithmic
 * @tags numeric, key value
 * @since 0.3.2
 */
public class KeyValUnifier<K, V extends Number> extends BaseNumberKeyValueOperator<K,V> implements Unifier<HashMap<K, V>>
{
  public HashMap<K, Double> mergedTuple = new HashMap<K, Double>();
  
  private volatile double sumVal = 0;
  
  /**
   * This is the output port which emits key value pairs which map keys to sums.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> mergedport = new DefaultOutputPort<HashMap<K, V>>();

  @Override
  public void process(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      Double val = mergedTuple.get(e.getKey());
      if (val == null) {
        mergedTuple.put(e.getKey(), e.getValue().doubleValue());
      }
      else {
        val += e.getValue().doubleValue();
        mergedTuple.put(e.getKey(), val);
      }
    }
  }

  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty()) {
      HashMap<K, V> stuples = new HashMap<K, V>();
      for (Map.Entry<K, Double> e: mergedTuple.entrySet()) {
        stuples.put(e.getKey(), getValue(e.getValue()));
      }
      if(stuples.containsKey("Total")){
        Double val = stuples.get("Total").doubleValue();
     
        if(sumVal >= val){
          stuples.remove("Total");
        } else {
          sumVal = stuples.get("Total").doubleValue();
        }
        
      }
      
      mergedport.emit(stuples);
      mergedTuple = new HashMap<K, Double>();
    }
  }

  /**
   * a no-op
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
