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

   event.setTs(this.getTs());
   event.setWeatherID(this.getWeatherID());
   event.setCity(this.getCity());
   event.setCount(this.getCount());
    return event;
  }

  @Override
  public String toString()
  {
    return "WeatherEvent [count=" + count + ", key=" + key + ", timeUnit=" + timeUnit + ", hashCode()=" + hashCode()
        + ", getTs()=" + getTs() + ", getWeatherID()=" + getWeatherID() + ", getCity()=" + getCity() + ", toString()="
        + super.toString() + ", getClass()=" + getClass() + "]";
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
