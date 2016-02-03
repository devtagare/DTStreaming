/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.dt.weather.counter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.UnifierHashMapSumKeys;

@OperatorAnnotation(partitionable = true)
public class KeyValChangeAggregator<K, V extends Number> extends BaseNumberKeyValueOperator<K, V> implements Operator
{

  private volatile HashMap<K, MutableDouble> emitMap = new HashMap<K, MutableDouble>();

  HashMap<K, MutableDouble> toEmit = new HashMap<K, MutableDouble>();

  /**
   * Sums key map.
   */
  protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();

  /**
   * Cumulative sum flag.
   */
  protected boolean cumulative = false;

  /**
   * Input port that takes key value pairs and adds the values for each key.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * For each tuple (a key value pair) Adds the values for each key.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      MutableDouble val = sums.get(key);
      if (val == null) {

        val = new MutableDouble(tuple.getValue().doubleValue());

        emitMap.put(cloneKey(key), val);

      } else {
        //Update a value already present in the map

        val.setValue(val.doubleValue() + tuple.getValue().doubleValue());

        emitMap.put(key, val);

      }
      //Add the updated values to the sums map 
      sums.put(cloneKey(key), val);
    }

    /**
     * Stream codec used for partitioning.
     */
    @Override
    public StreamCodec<KeyValPair<K, V>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }

  };

  /*
   * Output port to emit the aggregates if they have changed
   * */

  public final transient DefaultOutputPort<HashMap<K, MutableDouble>> alert = new DefaultOutputPort<HashMap<K, MutableDouble>>();

  /**
   * Output sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, MutableDouble>> sum = new DefaultOutputPort<HashMap<K, MutableDouble>>()
  {
    
    @Override
    public Unifier<HashMap<K, MutableDouble>> getUnifier()
    {
      KeyValUnifier<K, MutableDouble> unifierHashMapSumKeys = new KeyValUnifier<K, MutableDouble>();
      unifierHashMapSumKeys.setType(MutableDouble.class);
      
      return unifierHashMapSumKeys;
    }

  };

  /**
   * Get cumulative flag.
   * 
   * @return cumulative flag.
   */
  public boolean isCumulative()
  {
    return cumulative;
  }

  /**
   *
   * @param cumulative
   */
  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }

  /**
   * Emits on all ports that are connected. Data is pre-computed during process
   * on input port and endWindow just emits it for each key. Clears the internal
   * data.
   */
  @Override
  public void endWindow()
  {
    toEmit.put((K)"Total", new MutableDouble(sums.entrySet().size()));
    sum.emit(toEmit);
    emitChangedAggregates();
    clearCache();
  }

  /*
   * Emitts the changed aggregates
   * */

  public void emitChangedAggregates()
  {
    alert.emit(emitMap);
    emitMap.clear();

  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  public void clearCache()
  {
    if (cumulative) {
      for (Map.Entry<K, MutableDouble> e : sums.entrySet()) {
        MutableDouble val = e.getValue();

      }
    } else {
      sums.clear();
    }
  }

}
