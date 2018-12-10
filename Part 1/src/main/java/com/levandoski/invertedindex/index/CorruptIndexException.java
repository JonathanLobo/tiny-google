package com.levandoski.invertedindex.index;

/**
 * Thrown when some inconsistency in the index is found
 */
public class CorruptIndexException extends Exception{
	public CorruptIndexException(String msg) {
		super(msg);
	}

	public CorruptIndexException(Exception e) {
		super(e);
	}
}
