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
  private int weatherID;
  private String city;
  private String description;
 
  public String getDescription()
  {
    return description;
  }

  public void setDescription(String description)
  {
    this.description = description;
  }

  @JsonIgnore
  private int key;

  private TimeUnit timeUnit;
  public static final int DIMENSION_WEATHER_DESCRIPTION = 1 << 2;

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 89 * hash + this.weatherID;
    return hash;
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
    if (this.description != eventChk.description) {
      return false;
    }

    return true;
  }
  
  @Override
  public String toString()
  {
    return "WeatherBase [weatherID=" + weatherID + ", city=" + city + ", description=" + description + ", key=" + key
        + ", timeUnit=" + timeUnit + "]";
  }

}
