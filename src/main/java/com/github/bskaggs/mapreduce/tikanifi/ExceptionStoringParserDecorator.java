package com.github.bskaggs.mapreduce.tikanifi;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.utils.ExceptionUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class ExceptionStoringParserDecorator extends ParserDecorator{

	public ExceptionStoringParserDecorator(Parser parser) {
		super(parser);
	}

	private static final long serialVersionUID = 1L;
    
	/**
     * Copied/modified from WriteOutContentHandler.  Couldn't make that 
     * static, and we need to have something that will work 
     * with exceptions thrown from both BodyContentHandler and WriteOutContentHandler
     * @param t
     * @return
     */
    private boolean isWriteLimitReached(Throwable t) {
        if (t.getMessage() != null && 
                t.getMessage().indexOf("Your document contained more than") == 0) {
            return true;
        } else {
            return t.getCause() != null && isWriteLimitReached(t.getCause());
        }
    }
    
    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) {
        try {
            super.parse(stream, handler, metadata, context);
        } catch (SAXException e) {
            boolean wlr = isWriteLimitReached(e);
            if (wlr == true) {
                metadata.add(RecursiveParserWrapper.WRITE_LIMIT_REACHED, "true");
            } else {
            	String trace = ExceptionUtils.getStackTrace(e);
            	metadata.set(RecursiveParserWrapper.EMBEDDED_EXCEPTION, trace);
            }
        } catch (IOException|TikaException e) {
        	String trace = ExceptionUtils.getStackTrace(e);
            metadata.set(RecursiveParserWrapper.EMBEDDED_EXCEPTION, trace);
        }
    }        
}
