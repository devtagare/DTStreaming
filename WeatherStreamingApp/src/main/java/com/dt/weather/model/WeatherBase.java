package com.dt.weather.model;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnore;

public class WeatherBase implements Serializable
{
  
  /**
   * 
   */
  private static final long serialVersionUID = 5528471313726133689L;
  private long ts;
  private int weatherID;
  private String city;
 
  @JsonIgnore
  private int key;

  private TimeUnit timeUnit;
  public static final int DIMENSION_WEATHER_ID = 1 << 2;

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 89 * hash + this.weatherID;
    return hash;
  }
  
  public long getTs()
  {
    return ts;
  }
  public void setTs(long ts)
  {
    this.ts = ts;
  }
  public int getWeatherID()
  {
    return weatherID;
  }
  public void setWeatherID(int weatherID)
  {
    this.weatherID = weatherID;
  }
  public String getCity()
  {
    return city;
  }
  public void setCity(String city)
  {
    this.city = city;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }

    final WeatherBase eventChk = (WeatherBase) obj;
    if (this.weatherID != eventChk.weatherID) {
      return false;
    }

    if (this.ts != eventChk.ts) {
      return false;
    }

    return true;
  }
  
  @Override
  public String toString()
  {
    return "WeatherBase [ts=" + ts + ", weatherID=" + weatherID + ", city=" + city + ", key=" + key + ", timeUnit="
        + timeUnit + "]";
  }

}
