package com.levandoski.invertedindex.document;

/**
 * Represent a single word(token) contained in a document, also called term,
 */
public class Term {

	private String fieldName;
	public String token;

	public Term(final String fieldName, final String token) {
		this.token = token;
		this.fieldName = fieldName;
	}

	public String getToken() {
		return token;
	}

	public String getFieldName() {
		return fieldName;
	}
}
