package com.levandoski.invertedindex;


import com.levandoski.invertedindex.index.CorruptIndexException;
import com.levandoski.invertedindex.index.Hit;
import com.levandoski.invertedindex.index.IndexReader;
import com.levandoski.invertedindex.store.TxtFileDirectory;
import com.levandoski.invertedindex.util.Benchmark;
import com.levandoski.invertedindex.util.Logger;
import com.levandoski.invertedindex.document.Document;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

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
	public String printHits(TreeSet[] results, String[] args) {
		String out = "";

		TreeSet<Hit> termHits;
		String term;
		Iterator it;
		Hit hit;
		Boolean flag = false;

		List<Document> docs = new ArrayList<Document>();
		Document compDoc;

		int counter;

		for(int i=0; i < results.length; i++) {	// iterate through all keyword results lists
			term = args[i];
			termHits = results[i];
			
			if (termHits != null) {
				it = termHits.descendingSet().iterator();

				while (it.hasNext()) {
					hit = (Hit) it.next();
					for (int j = 0; j < docs.size(); j++) {
						compDoc = docs.get(j);
						if (hit.document().getDocumentId() == compDoc.getDocumentId()) {
							docs.get(j).setDocumentScore(compDoc.getDocumentScore() + hit.score());
							flag = true;
						}
					}
					if (flag == false) {	// was not in list
						docs.add(hit.document());
						docs.get(docs.indexOf(hit.document())).setDocumentScore(hit.score());
					}
					flag = false;
				}
			}
		}

		Collections.sort(docs, Collections.reverseOrder());

		for(int i = 0; i < docs.size(); i++) {
			out = out.concat((i+1) + " - \t" + docs.get(i).getDocumentScore() + " - \t" +  docs.get(i).fields().get("title").data() + "\n");
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
			System.out.println(searcher.printHits(results, args));
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
