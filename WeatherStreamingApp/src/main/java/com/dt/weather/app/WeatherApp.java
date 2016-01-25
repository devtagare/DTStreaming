/**
 * Put your copyright and license info here.
 */
package com.dt.weather.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.lib.math.SumKeyVal;
import com.dt.weather.constants.WeatherConstants;
import com.dt.weather.counter.KeyValChangeAlert;
import com.dt.weather.event.convertor.SinglePortWeatherEventConvertor;
import com.dt.weather.input.SimpleFileReader;

@ApplicationAnnotation(name = "WeatherApp")
public class WeatherApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, conf.get(WeatherConstants.APP_NAME, "WeatherApp"));

    /*Add the File reader*/

    SimpleFileReader fileReader = dag.addOperator("FileReader", new SimpleFileReader());

    fileReader.setDirectory(conf.get(WeatherConstants.INPUT_DIRECTORY_PATH,
        "/Users/dev/workspace/mydtapp/src/test/resources/data/"));
   
    //Uncomment the rename logic in simple file reader when the regex works
  //  fileReader.getScanner().setFilePatternRegexp("\\*.json");

    fileReader.setScanIntervalMillis(0);
    fileReader.setEmitBatchSize(1);

    /*Add the Event convertor*/

    SinglePortWeatherEventConvertor eventConvertor = dag.addOperator("WeatherEventConv",
        new SinglePortWeatherEventConvertor());
    
 //   DefaultConverter defaultConv = dag.addOperator("DefaultConverter", new DefaultConverter());

    KeyValChangeAlert<String, Integer> changeNotifier = dag.addOperator("ChangeNotifier", new KeyValChangeAlert<String, Integer>());

    changeNotifier.setAbsoluteThreshold(1);

    //TODO- add the partitioner code snippet here
    //TODO - change the locality of the converter with the uniqCounter

    //Add the overall counter
    SumKeyVal<String, Integer> counter = dag.addOperator("GlobalCounter", new SumKeyVal<String, Integer>());
    counter.setType(Integer.class);
    counter.setCumulative(true);
    
    dag.setAttribute(counter, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<SumKeyVal<String,Integer>>(3));

    //Kafka output's
    KafkaSinglePortOutputOperator<Object, Object> kafkaOutputOperator = dag.addOperator("KafkaOutputUniques",
        new KafkaSinglePortOutputOperator<Object, Object>());
    kafkaOutputOperator.setConfigProperties(getProducerProperties(conf));

    kafkaOutputOperator.setTopic(conf.get(WeatherConstants.TOPIC, "counter"));

    System.out.println("heremoe");

    /*Assemble the DAG*/

    dag.addStream("InputRecords", fileReader.output, eventConvertor.data).setLocality(Locality.THREAD_LOCAL);

    dag.addStream("Global Counter", eventConvertor.output, counter.data);

    dag.addStream("Change Notifier", counter.sum, changeNotifier.data);
    
    dag.addStream("Convert Output", changeNotifier.alert, kafkaOutputOperator.inputPort).setLocality(
        Locality.CONTAINER_LOCAL);

//    dag.addStream("KafkaGLobalCountsWriter", defaultConv.output, kafkaOutputOperator.inputPort).setLocality(
//        Locality.CONTAINER_LOCAL);

  }

  private Properties getProducerProperties(Configuration conf)
  {
    String brokerList = conf.get(WeatherConstants.BROKERSET, "localhost:9092");
    String metaData = conf.get(WeatherConstants.META_DATA_REFRESH, "60000");
    // TODO: get rid of hard coded keys
    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    props.put("topic.metadata.refresh.interval.ms", metaData);
    props.setProperty("producer.type", "async");
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("queue.buffering.max.ms", "200");
    props.setProperty("queue.buffering.max.messages", "10");
    props.setProperty("batch.num.messages", "5");

    return props;
  }

  public static Configuration readPropertiesFile(String fileName)
  {
    Configuration config = new Configuration(false);

    Properties prop = new Properties();
    InputStream input = null;

    try {

      input = new FileInputStream(fileName);

      prop.load(input);

      for (Entry<Object, Object> entry : prop.entrySet()) {
        String key = entry.getKey().toString();
        String value = entry.getValue().toString();
        config.set(key, value);

      }

    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return config;
  }

  public static void main(String[] args) throws Exception
  {

    LocalMode lma = LocalMode.newInstance();
    //  Configuration conf = new Configuration(false);

    Configuration conf = new Application()
        .readPropertiesFile("/Users/dev/workspace/mydtapp/src/test/resources/localmode.properties");
    // conf.addResource(.getClass().getResourceAsStream("/META-INF/properties.xml"));

    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run(10000);

  }

}
