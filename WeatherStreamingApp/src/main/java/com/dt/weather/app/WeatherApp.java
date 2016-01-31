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
import com.dt.weather.constants.WeatherConstants;
import com.dt.weather.converter.OutputConverter;
import com.dt.weather.counter.KeyValChangeAggregator;
import com.dt.weather.input.JSONFileInputOperator;

@ApplicationAnnotation(name = "WeatherApp")
public class WeatherApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, conf.get(WeatherConstants.APP_NAME, "WeatherApp"));

    /*Add the File reader*/

    JSONFileInputOperator fileReader = dag.addOperator("FileReader", new JSONFileInputOperator());

    fileReader.setDirectory(conf.get(WeatherConstants.INPUT_DIRECTORY_PATH,
        "/Users/dev/workspace/mydtapp/src/test/resources/data/"));

    fileReader.setMatchKey(conf.get(WeatherConstants.MATCH_KEY, "description"));

    fileReader
        .setProcessedDirPath("/Users/dev/checkout/personalGit/devel2/DTStreaming/WeatherStreamingApp/src/test/resources/data-processed/");

    fileReader.setScanIntervalMillis(0);
    fileReader.setEmitBatchSize(1);
    

    //Add the overall counter
    KeyValChangeAggregator<String, Integer> counter = dag.addOperator("GlobalCounter",
        new KeyValChangeAggregator<String, Integer>());
    counter.setType(Integer.class);
    counter.setCumulative(true);
    dag.setAttribute(counter, Context.OperatorContext.PARTITIONER,
        new StatelessPartitioner<KeyValChangeAggregator<String, Integer>>(5));

    OutputConverter<String, Integer> opConv = dag.addOperator("Converter", new OutputConverter<String, Integer>());

    //Kafka output's
    KafkaSinglePortOutputOperator<Object, Object> kafkaOutputOperator = dag.addOperator("KafkaOutputUniques",
        new KafkaSinglePortOutputOperator<Object, Object>());
    kafkaOutputOperator.setConfigProperties(getProducerProperties(conf));

    kafkaOutputOperator.setTopic(conf.get(WeatherConstants.TOPIC, "counter"));

    /*Assemble the DAG*/

    dag.addStream("InputRecords", fileReader.output, counter.data).setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("Unifier Output", counter.alert, opConv.data);
    
    dag.addStream("UnifierCumulative Output", counter.sum, opConv.dataCumulative);

    dag.addStream("Convert Output", opConv.output, kafkaOutputOperator.inputPort);

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

}
