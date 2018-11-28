package com.levandoski.invertedindex.store.file;

import com.levandoski.invertedindex.index.Posting;
import com.levandoski.invertedindex.store.codec.Codec;
import com.levandoski.invertedindex.index.CorruptIndexException;

import java.io.IOException;
import java.util.*;

/**
 * Handles Read/Write of postings file
 */
public class PostingsFile extends TxtFile {


	public PostingsFile(String path, Codec codec) {
		this.path = path;
		this.codec = codec;
	}


	protected HashMap<?,?> parseData() throws IOException, CorruptIndexException {
		HashMap<String, List<Posting>> postings = new HashMap<>();
		String rawData = null;
		//traverse the file and parse one by one the postings of every term using codec
		while ((rawData = this.reader.readLine()) != null) {
			Map.Entry<String, List<Posting>> entry = this.codec.readEntry(rawData);
			if (entry != null) {
				postings.put(entry.getKey(), entry.getValue());
			}
		}
		return postings;
	}

}
