package com.datastax.tika;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.datastax.demo.utils.FileUtils;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.tika.model.MetadataObject;
import com.datastax.tika.service.MetadataService;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private List<String> keywords = Arrays.asList("architecture", "white paper");

	public Main() {

		String fileLocation = PropertyHelper.getProperty("fileLocation", "src/main/resources/files");
		
		// Examples of using variables passed in using -DcontactPoints
		MetadataService service = new MetadataService(fileLocation);

		logger.info("Parsing Documents");

		Timer timer = new Timer();

		// Do something here.
		// For all docs			
		processFiles(service, fileLocation);

		processLinks(service);
		
		timer.end();
		logger.info("Test took " + timer.getTimeTakenSeconds() + " secs.");
		System.exit(0);
	}

	private void processLinks(MetadataService service) {
		List<String> list = FileUtils.readFileIntoList("links.txt");
		Set<String> set = new HashSet<String>(list); //Remove duplicates
		
		for (String url : set){
			try {
				MetadataObject metadata = service.processLink(new URL(url));
				service.insertMetadataObject(metadata);
			
			} catch (IOException | SAXException | TikaException | URISyntaxException e) {
				e.printStackTrace();
			} 
		}
	}

	private void processFiles(MetadataService service, String fileLocation) {
		List<File> files = listf(fileLocation);
		
		List<File> dirs = new ArrayList<File>();
		
		for (File file : files) {
			if (file.getName().startsWith(".")){
				logger.warn("Ignoring " + file.getAbsolutePath());
				continue;
			}
			
			if (file.isDirectory()){
				dirs.add(file);				
				service.mkdir(file);
				continue;
			}
			
			logger.info("Processing " + file.getAbsolutePath());
			try {
				MetadataObject metadata = service.processFile(file);
				service.sendFile(file, metadata);
				service.insertMetadataObject(metadata);
			
			} catch (IOException | SAXException | TikaException e) {
				e.printStackTrace();
			}
		}
		
		for (File dir : dirs){
			processFiles(service, dir.getAbsolutePath());
		}
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
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

}
