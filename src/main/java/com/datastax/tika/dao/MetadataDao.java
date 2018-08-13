package com.datastax.tika.dao;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.tika.model.MetadataObject;

public class MetadataDao {

	private Session session;

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
