package com.xxr.flink.test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class gotest {

	@Test
	void test() {
		String aString="\"_id\" : { \"$oid\" : \"5ae2cc89a5f7dd1e8896db7c\" }, \"class\" : \"go.GraphLinksModel\", \"linkFromPortIdProperty\"";
		String regex = "\"_id\"(.*?)\"class\"";
		String b=aString.replaceAll("\"_id(.*)\"class","\"class");
		
		
        System.out.println(b);
		System.out.println(aString);
		System.out.println("qq");
	}

}
