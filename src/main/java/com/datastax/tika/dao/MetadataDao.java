package com.datastax.tika.dao;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.tika.model.MetadataObject;

public class MetadataDao {

	private static Logger logger = LoggerFactory.getLogger(MetadataDao.class);
	private Session session;

	private static String keyspaceName = "tika";
	private static String testTable = keyspaceName + ".metadata";
	private List<KeyspaceMetadata> keyspaces;
	private Mapper<MetadataObject> metadataMapper;
		
	public MetadataDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();
		this.keyspaces = cluster.getMetadata().getKeyspaces();
		
		metadataMapper = new MappingManager(this.session).mapper(MetadataObject.class);
	}

	public void saveMetadataObject(MetadataObject metadata){
		
		metadataMapper.save(metadata);
	}
	
	public List<KeyspaceMetadata> getKeyspaces() {
		return keyspaces;
	}

}
