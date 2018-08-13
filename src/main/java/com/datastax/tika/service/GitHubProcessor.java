package com.datastax.tika.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.datastax.demo.utils.FileUtils;
import com.datastax.tika.model.MetadataObject;

public class GitHubProcessor extends TikaProcessor {

	private static Logger logger = LoggerFactory.getLogger(GitHubProcessor.class);
	
	public GitHubProcessor() {
		super();
		
		logger.info("Starting GitHubProcessor");

		processLinks();
	}
	
	private void processLinks() {
		List<String> list = FileUtils.readFileIntoList("links.txt");
		Set<String> set = new HashSet<String>(list); //Remove duplicates
		
		for (String url : set){
			try {
				MetadataObject metadata = processLink(new URL(url));
				insertMetadataObject(metadata);
			
			} catch (IOException | SAXException | TikaException | URISyntaxException e) {
				e.printStackTrace();
			} 
		}
	}
	
	public MetadataObject processLink(URL url) throws IOException, SAXException, TikaException, URISyntaxException {

		BodyContentHandler handler = new BodyContentHandler(-1);
		AutoDetectParser parser = new AutoDetectParser();
		Metadata metadata = new Metadata();
		MetadataObject metadataObject = new MetadataObject();
		URL originalURL = url;

		if (url.toString().contains("https://github.com/")) {
			String newUrl = url.toString().replaceAll("https://github.com/", "https://raw.githubusercontent.com/")
					.concat("/master/README.md");
			url = new URL(newUrl);
		}

		logger.info("Opening Stream to " + url.toString());

		try (InputStream stream = url.openStream()) {
			parser.parse(stream, handler, metadata);

			metadataObject.setLastModified(metadata.getDate(Metadata.LAST_MODIFIED));
			metadataObject.setContentType(metadata.get(Metadata.CONTENT_TYPE));
			metadataObject.setCreatedDate(metadata.getDate(Metadata.CREATION_DATE));
			metadataObject.setDocumentId(UUID.randomUUID().toString());
			metadataObject.setVersion(
					Double.parseDouble(metadata.get("pdf:PDFVersion") != null ? metadata.get("pdf:PDFVersion") : "0"));
			metadataObject.setContent(handler.toString());
			metadataObject.setLink(originalURL.toURI().toString());
			metadataObject.setType("URL");

			Map<String, String> metadataMap = new HashMap<String, String>();
			for (String name : metadata.names()) {
				metadataMap.put("md_" + name, metadata.get(name));
			}
			metadataObject.setMetadataMap(metadataMap);
			logger.info(metadata.toString());
		}

		return metadataObject;
	}

}
