package com.github.bskaggs.mapreduce.tikanifi.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.github.bskaggs.mapreduce.tikanifi.TikiNifiConverter;
import com.github.bskaggs.mapreduce.tikanifi.avro.TikaFile;

public class TikaTest {

	@Test
	public void testName() throws Exception {
		TikiNifiConverter tnc = new TikiNifiConverter("filename", true, false);
		
		File directory = new File(System.getProperty("user.home"), "projects/tika/tika-parsers/src/test/resources/test-documents");
		File[] files = directory.listFiles();
		for (File file : files) {
			if (!file.isFile()) {
				continue;
			}
	 		try(InputStream in = new FileInputStream(file)) {
	 			HashMap<String, String> attributes = new HashMap<>();
	 			attributes.put("filename", file.getAbsolutePath());
	 			System.out.println(attributes);
	 			TikaFile tf = tnc.parse(attributes, in);
	 			
	 			for (Map<String, List<String>> md : tf.getResources()) {
					for (Entry<String, List<String>> entry : md.entrySet()) {
						System.out.println(entry.getKey() + " : " + entry.getValue());
					}
					System.out.println();
				}
			
				System.out.println("***************************");
	 		}
		}
	}
}
