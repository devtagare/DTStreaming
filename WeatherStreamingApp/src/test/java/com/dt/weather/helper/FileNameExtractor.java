package com.dt.weather.helper;

import java.util.StringTokenizer;

import org.apache.tools.ant.types.CommandlineJava.SysProperties;

public class FileNameExtractor
{

  public static void main(String[] args)
  {
    String str = "file:/Users/dev/checkout/personalGit/devel/DTStreaming/WeatherStreamingApp/src/test/resources/data/WeatherInfoTest.1.json";
     
    String [] arr = str.split("/");
    
    System.out.println(arr[arr.length-1]);
    
    
  }
}
