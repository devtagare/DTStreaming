package com.dt.weather.helper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;


public class RegexTest
{
  
  @Test
  public void testRegex()
  {
    String filePatternRegexp="";
    
    String filePathStr="";
   
    RegexTest regexTest = new RegexTest();
    
    filePathStr="WeatherInfoTest.0.json";
    
    filePatternRegexp="WeatherInfoTest.[0-9]*.json";
    
    Assert.assertTrue(regexTest.checkRegex(Pattern.compile(filePatternRegexp),filePathStr));
   
    filePatternRegexp="WeatherInfoTest\\.[0-9]\\*\\.json";
    
    Assert.assertFalse(regexTest.checkRegex(Pattern.compile(filePatternRegexp),filePathStr));
    
    filePatternRegexp="WeatherInfoTest.[0-9]*.json";
    
    filePathStr="WeatherInfoTest.A.json";
    
    Assert.assertFalse(regexTest.checkRegex(Pattern.compile(filePatternRegexp),filePathStr));
    
    filePatternRegexp="WeatherInfoTest.[0-9]*.json";
    
    filePathStr="WeatherInfoTest.1.json.proc";
    
    Assert.assertFalse(regexTest.checkRegex(Pattern.compile(filePatternRegexp),filePathStr));
    
    
  }

  
  public boolean checkRegex(Pattern regex, String filePathStr){
  if (regex != null)
  {
    Matcher matcher = regex.matcher(filePathStr);
    if (!matcher.matches()) {
      return false;
    }
  }
  return true;
}
}
