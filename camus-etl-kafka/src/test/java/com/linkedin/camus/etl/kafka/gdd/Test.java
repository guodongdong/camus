package com.linkedin.camus.etl.kafka.gdd;

import java.util.Date;

import org.joda.time.DateTime;

public class Test {

	public static void main(String[] args) {
		 Long beginTimeStamp = (new DateTime()).minusDays(7).getMillis();
		 Date date = new Date(beginTimeStamp);
		 System.out.println(date);
	}
}
