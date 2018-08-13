package com.datastax.tika.service;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.tika.dao.MetadataDao;
import com.datastax.tika.model.MetadataObject;

public abstract class TikaProcessor {

	private static Logger logger = LoggerFactory.getLogger(TikaProcessor.class);
	
	MetadataDao dao;
	DSEFileSystemOperations ops = new DSEFileSystemOperations();
	Configuration conf = new Configuration();
	String startLocation;

	public TikaProcessor() {
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String dsefsLocation = PropertyHelper.getProperty("dsefsLocation", "localhost");
		String dsefsPort = PropertyHelper.getProperty("dsefsPort", "5598");
		this.dao = new MetadataDao(contactPointsStr.split(","));

		String hdfsPath = "dsefs://" + dsefsLocation + "/:" + dsefsPort;

		conf.set("fs.defaultFS", hdfsPath);
		conf.set("fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem");
	}

	public List<KeyspaceMetadata> getKeyspaces() {
		return dao.getKeyspaces();
	}

	public void sendFile(File source, MetadataObject metadata) {

		try {
			ops.addFile(source.getAbsolutePath(), source.getAbsolutePath().substring(this.startLocation.length()),
					getConf());
			metadata.setLink(source.getAbsolutePath().substring(this.startLocation.length()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void insertMetadataObject(MetadataObject metadata) {
		dao.saveMetadataObject(metadata);
	}

	public void mkdir(File file) {
		try {
			ops.mkdir(file.getAbsolutePath().substring(this.startLocation.length()), getConf());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Configuration getConf() {
		return conf;
	}
}
