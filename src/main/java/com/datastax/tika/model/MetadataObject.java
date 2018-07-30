package com.datastax.tika.model;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;


/*
 * 
 * create table if not exists metadata (
documentid text PRIMARY KEY,
created_date date,
last_modified date,
version double, 
encrypted boolean,
content_type text,
tags list<text>, 
content text 
);

 */
@Table(keyspace="tika", name="metadata")
public class MetadataObject {

	@PartitionKey
	private String documentId;
	
	@Column(name="created_date")
	private Date createdDate;
	
	@Column(name="last_modified")
	private Date lastModified;
	
	private Double version;
	private boolean encrypted;
	
	@Column(name="content_type")
	private String contentType;
	
	private String type;
	private String link;
	private List<String> tags;
	private String content;
	
	@Column(name="md_")
	private Map<String, String> metadataMap;
	
	public String getDocumentId() {
		return documentId;
	}
	public void setDocumentId(String documentId) {
		this.documentId = documentId;
	}
	public Date getCreatedDate() {
		return createdDate;
	}
	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}
	public Date getLastModified() {
		return lastModified;
	}
	public void setLastModified(Date lastModified) {
		this.lastModified = lastModified;
	}
	public Double getVersion() {
		return version;
	}
	public void setVersion(Double version) {
		this.version = version;
	}
	public boolean isEncrypted() {
		return encrypted;
	}
	public void setEncrypted(boolean encrypted) {
		this.encrypted = encrypted;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public List<String> getTags() {
		return tags;
	}
	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public Map<String, String> getMetadataMap() {
		return metadataMap;
	}
	public void setMetadataMap(Map<String, String> metadataMap) {
		this.metadataMap = metadataMap;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
}
