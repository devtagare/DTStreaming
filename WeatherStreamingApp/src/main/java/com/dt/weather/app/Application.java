/**
 * Put your copyright and license info here.
 */
package com.dt.weather.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.tools.ant.types.CommandlineJava.SysProperties;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.converter.MapToKeyHashValuePairConverter;
import com.datatorrent.lib.math.SumKeyVal;
import com.datatorrent.lib.stream.StreamMerger;
import com.datatorrent.lib.util.KeyValPair;
import com.dt.weather.constants.WeatherConstants;
import com.dt.weather.converter.DefaultConverter;
import com.dt.weather.converter.UniquesConverter;
import com.dt.weather.convertor.WeatherEventConvertor;
import com.dt.weather.input.SimpleFileReader;

@ApplicationAnnotation(name = "WeatherApp")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, conf.get(WeatherConstants.APP_NAME, "WeatherApp"));

    /*Add the File reader*/

    SimpleFileReader fileReader = dag.addOperator("FileReader", new SimpleFileReader());

    fileReader.setDirectory(conf.get(WeatherConstants.INPUT_DIRECTORY_PATH,
        "/Users/dev/workspace/mydtapp/src/test/resources/data/"));

    fileReader.setScanIntervalMillis(0);
    fileReader.setEmitBatchSize(1);

    /*Add the Event convertor*/

    WeatherEventConvertor eventConvertor = dag.addOperator("WeatherEventConv", new WeatherEventConvertor());

    DefaultConverter defaultConv = dag.addOperator("DefaultConv", new DefaultConverter());
    UniquesConverter uniqConv = dag.addOperator("UniqConv", new UniquesConverter());

    /*Add the uniques */
    UniqueCounter<KeyValPair<Integer, Integer>> uniqCount = dag.addOperator("UniquesCounter",
        new UniqueCounter<KeyValPair<Integer, Integer>>());
    //    @SuppressWarnings("rawtypes")
    MapToKeyHashValuePairConverter<KeyValPair<Integer, Integer>, Integer> converter = dag.addOperator("converter",
        new MapToKeyHashValuePairConverter());
    uniqCount.setCumulative(true);

    //Add the overall counter
    SumKeyVal<Integer, Integer> counter = dag.addOperator("GlobalCounter", new SumKeyVal<Integer, Integer>());
    counter.setType(Integer.class);
    counter.setCumulative(true);

    //Kafka output's
    KafkaSinglePortOutputOperator<Object, Object> kafkaOutputOperator = dag.addOperator("KafkaOutputUniques",
        new KafkaSinglePortOutputOperator<Object, Object>());
    kafkaOutputOperator.setConfigProperties(getProducerProperties(conf));

    kafkaOutputOperator.setTopic(conf.get(WeatherConstants.TOPIC, "counter"));

    KafkaSinglePortOutputOperator<Object, Object> kafkaOutputOperatorGlobal = dag.addOperator("KafkaOutputGlobal",
        new KafkaSinglePortOutputOperator<Object, Object>());

    kafkaOutputOperatorGlobal.setConfigProperties(getProducerProperties(conf));
    kafkaOutputOperatorGlobal.setTopic(conf.get(WeatherConstants.TOPIC, "counter"));

    System.out.println("heremoe");

    /*Assemble the DAG*/

    dag.addStream("InputRecords", fileReader.output, eventConvertor.data).setLocality(Locality.THREAD_LOCAL);

    dag.addStream("Uniques", eventConvertor.outputUnique, uniqCount.data);

    dag.addStream("Global Counter", eventConvertor.output, counter.data);

    dag.addStream("UniquesConv", uniqCount.count, converter.input);

    dag.addStream("KafkaConv", converter.output, uniqConv.data);

    dag.addStream("kafkaUniqWriter", uniqConv.output, kafkaOutputOperator.inputPort).setLocality(
        Locality.CONTAINER_LOCAL);

    dag.addStream("KafkaOutputGlobalCounts", counter.sum, defaultConv.data);
    dag.addStream("KafkaGLobalCountsWriter", defaultConv.output, kafkaOutputOperatorGlobal.inputPort).setLocality(
        Locality.CONTAINER_LOCAL);

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
