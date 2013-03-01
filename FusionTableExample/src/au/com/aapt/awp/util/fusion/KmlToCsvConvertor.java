package au.com.aapt.awp.util.fusion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

public class KmlToCsvConvertor {

	private static Log logger = LogFactory.getLog(KmlToCsvConvertor.class);
	
	private static String DATA_FILE_DELIMINATOR;
	
	private static String DATA_FILE_PATH;
	
	private static String SOURCE_KML_FILE_PATH;
	
	private static String ROOT_ELEMENT_NODE_NAME;
	
	private static List<String> FIELD_ELEMENT_NAMES;
	
	private static void loadProperty(){
		Properties prop = new Properties();
		ClassLoader loader = Thread.currentThread().getContextClassLoader();           
		InputStream stream = loader.getResourceAsStream("config.properties");
		try {			
			prop.load(stream);
		} catch (IOException e) {
			logger.fatal("Fail to load config.properties file", e);
		}
		SOURCE_KML_FILE_PATH = prop.getProperty("source.kml.file.path");
		DATA_FILE_PATH = prop.getProperty("data.file.path");
		DATA_FILE_DELIMINATOR = prop.getProperty("data.file.deliminator");
		ROOT_ELEMENT_NODE_NAME = prop.getProperty("kml.root.element.node.name");
		FIELD_ELEMENT_NAMES = Arrays.asList(prop.getProperty("kml.field.element.node.names").split(","));
	}

	private static DefaultHandler getEventHandler(final List<List<String>> output){
		return new DefaultHandler(){
			private StringBuilder value = new StringBuilder();
			boolean isInRootElement = false;
			boolean isInFieldElement = false;
			Map<String, String> fieldMap = new HashMap<String, String>();
			
			@Override
            public void startElement(String uri, String localName, String currentXmlElement, Attributes attributes) throws SAXException {
				String currentTag = stripNamespaceFromTag(currentXmlElement);
				if(ROOT_ELEMENT_NODE_NAME.equals(currentTag)){
					isInRootElement = true;
					fieldMap.clear();
				} else if(FIELD_ELEMENT_NAMES.contains(currentTag)){
					isInFieldElement = true;
					value.setLength(0);
				} else if(isInRootElement && isInFieldElement){
					value.append("<").append(currentXmlElement).append(">");
				}
            }
			
			@Override
		    public void endElement (String uri, String localName, String currentXmlElement) throws SAXException{
				String currentTag = stripNamespaceFromTag(currentXmlElement);
				if(ROOT_ELEMENT_NODE_NAME.equals(currentTag)){
					isInRootElement = false;
					List<String> row = new ArrayList<String>();
					for(String fieldName : FIELD_ELEMENT_NAMES)
						row.add(fieldMap.get(fieldName));
					output.add(row);
				} else if(FIELD_ELEMENT_NAMES.contains(currentTag)){
					isInFieldElement = false;
					fieldMap.put(currentXmlElement, value.toString());
				} else if(isInRootElement && isInFieldElement){
					value.append("</").append(currentXmlElement).append(">");
				}
		    }
			
            @Override
            public void characters(char ch[], int start, int length) throws SAXException {
                value = value.append(ch, start, length);
            }
            
            @Override
            public void endDocument(){
            	
            }
            
            private String stripNamespaceFromTag(String tagWithNamespace) {
                int beginIndex = tagWithNamespace.indexOf(":");
                return beginIndex >= 0 ? tagWithNamespace.substring(beginIndex + 1) : tagWithNamespace;
            }
		};
	}
	
	public static void main(String[] argv){
		loadProperty();
		writeToFile(parseKmlFile());
	}
	
	private static List<List<String>> parseKmlFile(){
		List<List<String>> result = new ArrayList<List<String>>();
		SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			SAXParser saxParser = factory.newSAXParser();
			DefaultHandler handler = getEventHandler(result);
			saxParser.parse(SOURCE_KML_FILE_PATH, handler);
			logger.info("Finish for parsing KML file");
		} catch (Exception e) {
			logger.error("Fail to parse KML file from "+SOURCE_KML_FILE_PATH, e);
		}

		return result;
	}
	
	private static void writeToFile(List<List<String>> data){
		File to = new File(DATA_FILE_PATH);
		Joiner joiner = Joiner.on(DATA_FILE_DELIMINATOR).useForNull("");
		StringBuilder sb = new StringBuilder();
		for(List<String> row : data){
			sb.append(joiner.join(row)).append(System.getProperty("line.separator"));
		}
		try {
			Files.write(sb.toString(), to, Charsets.UTF_8);
			logger.info("Finish for writing content to file "+DATA_FILE_PATH);
		} catch (IOException e) {
			logger.error("Fail to write content to file "+DATA_FILE_PATH, e);
		}
	}
}
