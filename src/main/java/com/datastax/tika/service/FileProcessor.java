package com.datastax.tika.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.tika.model.MetadataObject;

public class FileProcessor extends TikaProcessor {

	private static Logger logger = LoggerFactory.getLogger(FileProcessor.class);
	
	public FileProcessor() {
		super();
		logger.info("Starting GitHubProcessor");
		this.startLocation = PropertyHelper.getProperty("fileLocation", "src/main/resources/files");
		processFiles(startLocation);
	}
	
	private void processFiles(String fileLocation) {
		List<File> files = listf(fileLocation);
		
		List<File> dirs = new ArrayList<File>();
		
		for (File file : files) {
			if (file.getName().startsWith(".")){
				logger.warn("Ignoring " + file.getAbsolutePath());
				continue;
			}
			
			if (file.isDirectory()){
				dirs.add(file);				
				this.mkdir(file);
				continue;
			}
			
			logger.info("Processing " + file.getAbsolutePath());
			try {
				MetadataObject metadata = process(file.getAbsolutePath());
				sendFile(file, metadata);
				insertMetadataObject(metadata);
			
			} catch (IOException | SAXException | TikaException e) {
				e.printStackTrace();
			}
		}
		
		for (File dir : dirs){
			processFiles(dir.getAbsolutePath());
		}
	}


	public MetadataObject process(String fileName) throws IOException, SAXException, TikaException {

		File file = new File(fileName);
	
		BodyContentHandler handler = new BodyContentHandler(-1);
		AutoDetectParser parser = new AutoDetectParser();
		Metadata metadata = new Metadata();
		MetadataObject metadataObject = new MetadataObject();

		try (InputStream stream = FileUtils.openInputStream(file)) {
			parser.parse(stream, handler, metadata);

			metadataObject.setLastModified(metadata.getDate(Metadata.LAST_MODIFIED));
			metadataObject.setContentType(metadata.get(Metadata.CONTENT_TYPE));
			metadataObject.setCreatedDate(metadata.getDate(Metadata.DATE));
			metadataObject.setDocumentId(UUID.randomUUID().toString());
			metadataObject.setVersion(
					Double.parseDouble(metadata.get("pdf:PDFVersion") != null ? metadata.get("pdf:PDFVersion") : "0"));
			metadataObject.setContent(handler.toString());
			metadataObject.setLink(file.getAbsolutePath());
			metadataObject.setType("File");

			Map<String, String> metadataMap = new HashMap<String, String>();
			for (String name : metadata.names()) {
				metadataMap.put("md_" + name, metadata.get(name));
			}
			metadataObject.setMetadataMap(metadataMap);
			logger.info(metadata.toString());
		}

		return metadataObject;
	}


	public static List<File> listf(String directoryName) {
        File directory = new File(directoryName);

        List<File> resultList = new ArrayList<File>();

        // get all the files from a directory
        File[] fList = directory.listFiles();
        if (fList==null || fList.length == 0){
        	logger.info(directoryName + " directory is empty");
        	return resultList;
        }
        
        resultList.addAll(Arrays.asList(fList));
        
        for (File file : fList) {
            if (file.isFile()) {
                System.out.println(file.getAbsolutePath());
            } else if (file.isDirectory()) {
                resultList.addAll(listf(file.getAbsolutePath()));
            }
        }
        return resultList;
    } 

}
