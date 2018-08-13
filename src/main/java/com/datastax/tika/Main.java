package com.datastax.tika;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.FileUtils;
import com.datastax.demo.utils.Timer;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {

		logger.info("Parsing Documents");

		Timer timer = new Timer();

		List<String> list = FileUtils.readFileIntoList("processors");
		
		for (String processorClass : list){
			try {
				
				Class.forName(processorClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		timer.end();
		logger.info("Test took " + timer.getTimeTakenSeconds() + " secs.");
		System.exit(0);
	}



	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

}
