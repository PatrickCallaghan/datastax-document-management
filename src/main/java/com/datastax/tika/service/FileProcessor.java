package com.datastax.tika.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.tika.exception.TikaException;
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
		InputStream stream = FileUtils.openInputStream(file);
		return extractMetadata(stream, file.getAbsolutePath(), "File");
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
