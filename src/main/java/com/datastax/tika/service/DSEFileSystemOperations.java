package com.datastax.tika.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DSEFileSystemOperations {

	private static Logger logger = LoggerFactory.getLogger(DSEFileSystemOperations.class);

	public DSEFileSystemOperations() {

	}

	public void addFile(String source, String dest, Configuration conf) throws IOException {
		try (FileSystem fileSystem = FileSystem.get(conf)) {

			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

			// Create the destination path including the filename.
			if (dest.charAt(dest.length() - 1) != '/') {
				dest = dest + "/" + filename;
			} else {
				dest = dest + filename;
			}

			logger.info("Adding " + source + " to " + dest);

			// Check if the file already exists
			Path path = new Path(dest);

			if (fileSystem.exists(path)) {
				logger.info("File " + dest + " already exists");
				return;
			}

			// Create a new file and write data to it.
			try (FSDataOutputStream out = fileSystem.create(path);
				 InputStream in = new BufferedInputStream(new FileInputStream(new File(source)))) {
				IOUtils.copy(in, out);
			}
		}
	}

	public void readFile(String file, Configuration conf) throws IOException {
		try (FileSystem fileSystem = FileSystem.get(conf)) {
			Path path = new Path(file);
			if (!fileSystem.exists(path)) {
				logger.info("File " + file + " does not exists");
				return;
			}

			String filename = file.substring(file.lastIndexOf('/') + 1, file.length());
			try (FSDataInputStream in = fileSystem.open(path);
				 OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)))) {
				IOUtils.copy(in, out);
			}
		}
	}

	public void deleteFile(String file, Configuration conf) throws IOException {
		try (FileSystem fileSystem = FileSystem.get(conf)) {
			Path path = new Path(file);
			if (!fileSystem.exists(path)) {
				logger.info("File " + file + " does not exists");
				return;
			}

			fileSystem.delete(new Path(file), true);
		}
	}

	public void mkdir(String dir, Configuration conf) throws IOException {
		try (FileSystem fileSystem = FileSystem.get(conf)) {

			Path path = new Path(dir);
			if (fileSystem.exists(path)) {
				logger.info("Dir " + dir + " already exists");
				return;
			}

			logger.info("********************** Creating path " + path.getName() + " ********************** ");
			fileSystem.mkdirs(path);
		}
	}
}