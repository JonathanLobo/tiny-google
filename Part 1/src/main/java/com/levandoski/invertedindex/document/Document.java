package com.levandoski.invertedindex.document;

import java.util.HashMap;

/**
 * Represents a single document contained in the index
 */
public class Document {

	private long documentId;

	private final HashMap<String, Field> fields = new HashMap<>();

	public Document(long documentId) {
		this.setDocumentId(documentId);
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

	public void setDocumentId(long documentId) {
		this.documentId = documentId;
	}
}
