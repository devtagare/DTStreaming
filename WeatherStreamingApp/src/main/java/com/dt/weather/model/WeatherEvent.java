package com.dt.weather.model;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class WeatherEvent extends WeatherBase implements Serializable
{

  /**
   * 
   */
  private static final long serialVersionUID = 3920011464318495189L;
  
  private int count;
  
  private int key;
  private TimeUnit timeUnit;

  public WeatherEvent copyEvent() {
    WeatherEvent event = new WeatherEvent();

   
   event.setWeatherID(this.getWeatherID());
   event.setCity(this.getCity());
   event.setCount(this.getCount());
   event.setDescription(this.getDescription());
    return event;
  }

  @Override
  public String toString()
  {
    return "WeatherEvent [count=" + count + ", key=" + key + ", timeUnit=" + timeUnit + "]";
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }
  

}
