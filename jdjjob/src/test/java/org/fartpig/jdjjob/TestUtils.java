package org.fartpig.jdjjob;

import org.fartpig.jdjjob.TestCustomTableName.HelloWorldJob;

import junit.framework.TestCase;

public class TestUtils extends TestCase {

	public void testSerializationObj() {
		HelloWorldJob job = new HelloWorldJob("delayed_job");
		System.out.println(Utils.serializationObj(job));
	}

}
