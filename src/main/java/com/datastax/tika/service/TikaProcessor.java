package com.datastax.tika.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

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
	
	@SuppressWarnings("deprecation")
	MetadataObject extractMetadata(InputStream stream, final String link, final String type) throws TikaException, SAXException, IOException {
		BodyContentHandler handler = new BodyContentHandler(-1);
		AutoDetectParser parser = new AutoDetectParser();
		Metadata metadata = new Metadata();
		MetadataObject metadataObject = new MetadataObject();
 		parser.parse(stream, handler, metadata);
 		metadataObject.setLastModified(metadata.getDate(Metadata.LAST_MODIFIED));
		metadataObject.setContentType(metadata.get(Metadata.CONTENT_TYPE));
		metadataObject.setCreatedDate(metadata.getDate(Metadata.CREATION_DATE));
		metadataObject.setDocumentId(UUID.randomUUID().toString());
		metadataObject.setVersion(Double.parseDouble(metadata.get("pdf:PDFVersion") != null ? metadata.get("pdf:PDFVersion") : "0"));
		metadataObject.setContent(handler.toString());
		metadataObject.setLink(link);
		metadataObject.setType(type);
 		Map<String, String> metadataMap = new HashMap<String,String>();
		for (String name : metadata.names()){
			metadataMap.put("md_" + name, metadata.get(name));
		}
		metadataObject.setMetadataMap(metadataMap);
		logger.info(metadata.toString());
 		return metadataObject;
	}
}
