package com.levandoski.invertedindex.document;

import java.util.HashMap;

/**
 * Represents a single document contained in the index
 */
public class Document implements Comparable<Document> {

	private long documentId;
	private float score;

	private final HashMap<String, Field> fields = new HashMap<>();

	public Document(long documentId) {
		this.setDocumentId(documentId);
		this.setDocumentScore(0);
	}

	public Document() {
		this.setDocumentId(-1);
	}

	public HashMap<String, Field> fields() {
		return this.fields;
	}

	public void addField(final Field field) {
		this.fields.put(field.name(), field);
	}

	public long getDocumentId() {
		return documentId;
	}

	public float getDocumentScore() {
		return score;
	}

	public void setDocumentId(long documentId) {
		this.documentId = documentId;
	}

	public void setDocumentScore(float documentScore) {
		this.score = documentScore;
	}

	@Override
	public int compareTo(Document doc) {
		if (this.score == doc.score) {
			return 0;
		} else if (this.score > doc.score) {
			return 1;
		}
		return -1;
	}
}
