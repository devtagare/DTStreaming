/**
 * Put your copyright and license info here.
 */
package com.dt.weather.app;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

import com.dt.weather.app.WeatherApp;

/**
 * Test the DAG declaration in local mode.
 */
public class WeatherAppTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
     // Configuration conf = new Configuration(false);
      Configuration conf = new WeatherApp().readPropertiesFile("/Users/dev/checkout/personalGit/devel/DTStreaming/WeatherStreamingApp/src/test/resources/localmode.properties");
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new WeatherApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(500000); // runs for 60 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
