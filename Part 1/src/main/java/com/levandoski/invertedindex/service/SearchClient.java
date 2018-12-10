package com.levandoski.invertedindex.service;

import com.levandoski.invertedindex.util.Benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Calendar;

/**
 * Created by bik on 4/3/14.
 */
public class SearchClient {


	public void sendQuery(String[] query, int port) throws IOException {
		Socket socket = new Socket("127.0.0.1", port);
		BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		PrintWriter output = new PrintWriter(socket.getOutputStream(), false);

		for(int i=0; i<query.length; i++) {
			output.print(query[i]+"\n");
		}
		output.print("end_of_sequence"+"\n");
		output.flush();

		String line = "";
		while ((line = input.readLine()) != null) {
			System.out.println(line);
		}
		input.close();
		output.close();

	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("No query term specified!");
			System.exit(0);
		}
		Benchmark.getInstance().start("SearchClient");
		SearchClient client = new SearchClient();

		int port = SearchServer.DEFAULT_PORT;

		try {
			String[] query = args;
			String queryString = "";
			for(int i = 0; i < args.length; i++) {
				if(i == args.length - 1) {
					queryString = queryString + args[i];
				} else {
					queryString = queryString + args[i] + ", ";
				}
			}
			System.out.printf("sending query '%s' to server at port %d\n", queryString, port);
			client.sendQuery(query, port);
		} catch (IOException e) {
			System.out.println("can' connect with the server ");
		}
		Benchmark.getInstance().end("SearchClient");

		long t = Benchmark.getInstance().getTime("SearchClient");
		System.out.printf("\ntotal time for this query: %d milliseconds\n", t);
		long mem = Benchmark.getInstance().getMemory("SearchClient");
		System.out.printf("memory used: %f MB\n", (float) mem / 1024 / 1024);
	}
}
