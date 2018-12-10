package com.levandoski.invertedindex;


import com.levandoski.invertedindex.index.CorruptIndexException;
import com.levandoski.invertedindex.index.Hit;
import com.levandoski.invertedindex.index.IndexReader;
import com.levandoski.invertedindex.store.TxtFileDirectory;
import com.levandoski.invertedindex.util.Benchmark;
import com.levandoski.invertedindex.util.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Uses IndexReader to perform a search in the index
 */
public class Searcher {

	Logger log;

	IndexReader reader;


	public  Searcher() {
		this.log = new Logger();
	}

	/**
	 * open the index
	 * @throws IOException
	 * @throws CorruptIndexException
	 */
	public IndexReader openIndexReader() throws IOException, CorruptIndexException {
		//resolve directory path
		String currentDirectory = new File("").getAbsolutePath();
		String directoryPath = currentDirectory.concat("/index/");
		System.out.printf("Reading index files from this directory: %s \n\n", directoryPath);

		this.reader = new IndexReader(new TxtFileDirectory(directoryPath));
		this.reader.open();
		return reader;
	}

	/**
	 * perform the search of one single term in the index
	 * @param term
	 * @return
	 */
	public TreeSet[] search(String[] terms) {
		TreeSet<Hit> resultSet = null;
		IndexReader reader = null;
		TreeSet[] resultArray = new TreeSet[terms.length];
		try {
			//search for term occurrences in body field
			for(int i=0; i < terms.length; i++) {
				if(terms[i] != null) {
					System.out.println(terms[i]);
					resultSet = this.reader.search(Indexer.FieldName.BODY.toString(), terms[i]);
					resultArray[i] = resultSet;
				}
			}
		} catch (IOException e) {
			this.log.error("There was an IO error reading the index files ", e);
		} catch (CorruptIndexException e) {
			this.log.error("Index data is corrupt ", e);
		} catch (IllegalArgumentException e) {
			Logger.getInstance().error(e.getMessage());
		} finally {
			//close open resources
			if (reader != null) {
				reader.close();
			}
		}
		return resultArray;
	}

	/**
	 * show the list of results
	 * @param hits
	 * @param term
	 */
	public String printHits(TreeSet<Hit> hits, String term) {
		String out = "";
		if (hits == null || hits.isEmpty()) {
			//return String.format("No documents found matching the term %s \n", term);
		} else {
			out = String.format("%d Documents found matching the term %s: \n", hits.size(), term);

			Iterator it = hits.descendingSet().iterator();
			int i = 1;
			while(it.hasNext()) {
				Hit hit = (Hit) it.next();
				out = out.concat(String.format("%d - %f - %s \n", i++, hit.score(), hit.document().fields().get("title").data()));
			}
		}
		return out;
	}


	public static void main(String[] args) {

		if (args.length == 0) {
			System.out.println("No query term specified!");
			System.exit(0);
		}

		Benchmark.getInstance().start("Searcher.main");
		try {
			Searcher searcher = new Searcher();
			searcher.openIndexReader();
			TreeSet[] results = searcher.search(args);
			for(int i = 0; i < args.length; i++) {
				System.out.println(searcher.printHits(results[i], args[i]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		}
		Benchmark.getInstance().end("Searcher.main");

		long t = Benchmark.getInstance().getTime("Searcher.main");
		System.out.printf("\ntotal time for this query: %d milliseconds\n", t);
		long mem = Benchmark.getInstance().getMemory("Searcher.main");
		System.out.printf("memory used: %f MB\n", (float) mem / 1024 / 1024);
	}
}
