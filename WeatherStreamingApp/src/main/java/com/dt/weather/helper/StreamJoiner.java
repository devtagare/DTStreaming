package com.dt.weather.helper;
//package com.example.helper;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import com.datatorrent.api.DefaultInputPort;
//import com.datatorrent.api.DefaultOutputPort;
//import com.datatorrent.api.StreamCodec;
//import com.datatorrent.common.util.BaseOperator;
//import com.datatorrent.lib.algo.UniqueCounter;
//import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
//import com.datatorrent.lib.util.KeyValPair;
//import com.example.model.CounterPair;
//
//public class StreamJoiner extends BaseOperator
//{
//
//  private transient HashMap<Integer, Integer> globalCounterMap;
//
//  private transient HashMap<Integer, Integer> uniques;
//
//  public StreamJoiner()
//  {
//    globalCounterMap = new HashMap<Integer, Integer>();
//  }
//
//  public final transient DefaultInputPort<HashMap<KeyValPair<Integer, Integer>,Integer>> uniquesInput = new DefaultInputPort <HashMap<KeyValPair<Integer, Integer>,Integer>>()
//  {
//    @Override
//    public void process(HashMap<KeyValPair<Integer, Integer>,Integer> tuple)
//    {
//      compute(tuple);
//    }
//
//    @Override
//    public StreamCodec<HashMap<KeyValPair<Integer, Integer>,Integer>> getStreamCodec()
//    {
//      return getAggregateStreamCodec();
//    }
//
//  };
//
//  public final transient DefaultInputPort<HashMap<KeyValPair<Integer, Integer>,Integer>> totalCountInput = new DefaultInputPort<HashMap<KeyValPair<Integer, Integer>,Integer>>()
//  {
//    @Override
//    public void process(HashMap<KeyValPair<Integer, Integer>,Integer> tuple)
//    {
//      compute(tuple);
//    }
//
//    @Override
//    public StreamCodec<HashMap<KeyValPair<Integer, Integer>,Integer>> getStreamCodec()
//    {
//      return getAggregateStreamCodec();
//    }
//
//  };
//
//  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
//
//  private void compute(HashMap<KeyValPair<Integer, Integer>,Integer> tuple)
//  {
//    if (globalCounterMap.containsKey(tuple.getKey())) {
//      globalCounterMap.put(tuple.getKey(), globalCounterMap.get(tuple.getKey()) + tuple.getValue());
//    } else {
//      globalCounterMap.put(tuple.getKey(), tuple.getValue());
//      uniques.put(tuple.getKey(), tuple.getValue());
//    }
//  }
//
//  public static class AggregateStreamCodec extends KryoSerializableStreamCodec<KeyValPair<Integer, Integer>>
//  {
//
//    @Override
//    public int getPartition(HashMap<KeyValPair<Integer, Integer>,Integer> t)
//    {
//      // get current aggregator index type from aggIndexPartionTypeMap
//
//      // note that keys used for topN cannot be included
//      int hash = 5;
//      hash = 89 * hash + t.getKey();
//
//      return hash;
//    }
//  }
//
//  protected StreamCodec<KeyValPair<Integer, Integer>> getAggregateStreamCodec()
//  {
//
//    return new AggregateStreamCodec();
//  }
//
//  @Override
//  public void endWindow()
//  {
//    //Flush the global counts
//    for (Map.Entry<Integer, Integer> entry : globalCounterMap.entrySet()) {
//      Integer key = entry.getKey();
//      Integer value = entry.getValue();
//
//      String outputTuple = "<" + key + "," + value + ">";
//
//      output.emit(outputTuple);
//    }
//
//    if (uniques.size() > 0) {
//      for (Map.Entry<Integer, Integer> entry : globalCounterMap.entrySet()) {
//        Integer key = entry.getKey();
//        Integer value = entry.getValue();
//        
//        if(value>=1){
//
//          String outputTuple = "<" + key + "," + value + ">";
//
//          output.emit(outputTuple);
//        }
//      }
//    }
//
//  }
//
//}
