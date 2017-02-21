package com.github.bskaggs.mapreduce.tikanifi;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.bskaggs.mapreduce.tikanifi.avro.TikaFile;

public class TikaNifiMapper extends Mapper<Map<String, String>, InputStream, AvroKey<TikaFile>, NullWritable> {
	
	private final static String BASE = TikaNifiMapper.class.getPackage().getName();
	
	private final static String FILENAME_ATTRIBUTE = BASE + ".filename_attribute";
	private final static String REMOVE_CONTENT = BASE + ".remove_content";
	private final static String TRIM_CONTENT = BASE + ".trim_content";

	private TikiNifiConverter converter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String filenameAttribute = conf.get(FILENAME_ATTRIBUTE);
		boolean removeContent = conf.getBoolean(REMOVE_CONTENT, false);
		boolean trimContent = conf.getBoolean(TRIM_CONTENT, false);
		
		
		converter = new TikiNifiConverter(filenameAttribute, removeContent, trimContent);
	}
	
	@Override
	protected void map(Map<String, String> key, InputStream value, Context context) throws IOException, InterruptedException {
		try {
			AvroKey<TikaFile> avroKey = new AvroKey<TikaFile>(converter.parse(key, value));
			context.write(avroKey, NullWritable.get());
		} catch (IOException e) {
			List<String> causes = new ArrayList<>();
			Throwable current = e;
			do {
				causes.add(current.getClass().getName());
				current = current.getCause();
			} while (current != null);
			
			context.getCounter(BASE + ".errors", causes.toString()).increment(1);
		}
	}
}
