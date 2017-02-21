package com.github.bskaggs.mapreduce.tikanifi;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.xmlbeans.impl.xb.xsdschema.WhiteSpaceDocument.WhiteSpace;
import org.xml.sax.SAXException;

import com.github.bskaggs.mapreduce.tikanifi.avro.TikaFile;

public class TikiNifiConverter {
	private final static String TIKA_CONTENT_NAME = RecursiveParserWrapper.TIKA_CONTENT.getName();
	private final String filenameField;
	private final boolean removeContent;
	final private ParseContext context = new ParseContext();
	final private Parser parser = new ExceptionStoringParserDecorator(new AutoDetectParser());
	//final private Parser parser = new AutoDetectParser();
	
	final private RecursiveParserWrapper wrapper = new RecursiveParserWrapper(parser, new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1));
	final private boolean trimContent;


	public TikiNifiConverter(String filenameField, boolean removeContent, boolean trimContent) {
		super();
		this.filenameField = filenameField;
		this.removeContent = removeContent;
		this.trimContent = trimContent;

	}

	final static Pattern whitespace = Pattern.compile("\\s+");
	final static Pattern emptyLines = Pattern.compile("(^\\s*$)+", Pattern.MULTILINE);
	final static Pattern trimmer = Pattern.compile("(^\\s+)|(\\s+$)");
	

	public TikaFile parse(Map<String, String> key, InputStream value) throws IOException {
		final Metadata metadata = new Metadata();
		
		
		if (filenameField != null) {
			metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, filenameField);
		}

		Map<String, String> attributes = new HashMap<>(key);
		

		//todo tesseract
		try (TikaInputStream tikaInputStream = TikaInputStream.get(value)) {
		    wrapper.parse(tikaInputStream, null, metadata, context);
			
			List<Map<String, List<String>>> resources = new ArrayList<>(wrapper.getMetadata().size());

			for (Metadata md : wrapper.getMetadata()) {
				Map<String, List<String>> resource = new HashMap<>();
				for (String name : md.names()) {
					if (removeContent && name.equals(TIKA_CONTENT_NAME)) {
						continue;
					} else if (trimContent && name.equals(TIKA_CONTENT_NAME)) {
						String[] values = md.getValues(name);
						String[] trimmed = new String[values.length];
						for (int i = 0; i < values.length; i++) {
							String text = whitespace.matcher(values[i]).replaceAll(" ");
							text = emptyLines.matcher(text).replaceAll("\n\n");
							text = trimmer.matcher(text).replaceAll("");
							trimmed[i] = text;
						}
						resource.put(name, Arrays.asList(trimmed));
					} else {
						resource.put(name, Arrays.asList(md.getValues(name)));
					}
				}
				resources.add(resource);
			}

			return TikaFile.newBuilder().setAttributes(attributes).setResources(resources).build();
		} catch (Exception e) {
            throw new IOException(e);
        } finally {
        	wrapper.reset();
        }
	}
}
