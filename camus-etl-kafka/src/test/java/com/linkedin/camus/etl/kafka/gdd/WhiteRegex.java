package com.linkedin.camus.etl.kafka.gdd;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class WhiteRegex {

	public static void main(String[] args) {
		
		Set<String> whiteListTopics = new HashSet<String>();
		whiteListTopics.add("gdd");
		Set<String> topics = new HashSet<String>();
		topics.add("%RETRY%PullConsumerGroup4");
		topics.add("gdd");
		topics.add("ccc");
		if(!whiteListTopics.isEmpty()){
			topics = filterWhitelistTopics(topics, whiteListTopics);
		}
		for (String string : topics) {
			System.out.println(string);
		}
	}
	public static Set<String> filterWhitelistTopics(Set<String> topicLists,
		      Set<String> whiteListTopics) {
		    Set<String> filteredTopics = new HashSet<String>();
		    String regex = createTopicRegEx(whiteListTopics);
		    for (String topic : topicLists) {
		      if (Pattern.matches(regex, topic)) {
		        filteredTopics.add(topic);
		      } else {
		        System.out.println("Discarding topic : " + topic);
		      }
		    }
		    return filteredTopics;
	}
	public static String createTopicRegEx(Set<String> topicsSet) {
	    String regex = "";
	    StringBuilder stringbuilder = new StringBuilder();
	    for (String whiteList : topicsSet) {
	      stringbuilder.append(whiteList);
	      stringbuilder.append("|");
	    }
	    regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1) + ")";
	    Pattern.compile(regex);
	    return regex;
	  }
}
