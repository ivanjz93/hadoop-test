package org.dan.service;

public interface NameNodeProtocol {
	
	public static final long versionID = 100L;
	
	String getMetaData(String path);
}
