package com.datastax.tika.webservice;

import java.text.SimpleDateFormat;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.tika.service.MetadataService;

@WebService
@Path("/")
public class MetadataWS {

	private Logger logger = LoggerFactory.getLogger(MetadataWS.class);
	private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");

	//Service Layer.
	private MetadataService service = new MetadataService("");
	
	@GET
	@Path("/get/keyspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements() {
				
		return null;
	}
	
	
}
