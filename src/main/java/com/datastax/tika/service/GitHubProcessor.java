package com.datastax.tika.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
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
	
		URL originalURL = url;
		if (url.toString().contains("https://github.com/")) {
			String newUrl = url.toString().replaceAll("https://github.com/", "https://raw.githubusercontent.com/")
					.concat("/master/README.md");
			url = new URL(newUrl);
		}

		logger.info("Opening Stream to " + url.toString());

		InputStream stream = url.openStream();
		return extractMetadata(stream, originalURL.toURI().toString(), "URL");
	}

}
