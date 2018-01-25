package org.dan.service;

public class NameNodeProtocolImp implements NameNodeProtocol {

	@Override
	public String getMetaData(String path) {
		return path + "3 {mqS1, mqS2, mqS3}";
	}

}
