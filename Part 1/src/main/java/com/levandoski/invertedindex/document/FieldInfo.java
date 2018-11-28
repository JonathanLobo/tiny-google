package com.levandoski.invertedindex.document;

import com.levandoski.invertedindex.parse.TextParser;

/**
 * Represents configuration for a concrete type of field
 */
public class FieldInfo {

	public static final String INDEXED = "indexed";
	public static final String STORED = "stored";

	protected static final boolean INDEXED_DEFAULT = true;
	protected static final boolean STORED_DEFAULT = false;
	protected static final boolean TOKENIZED_DEFAULT = true;
	protected boolean indexed;
	protected boolean stored;
	protected boolean tokenized;
	private Class parser = null;

	public FieldInfo() {
		this.indexed = FieldInfo.INDEXED_DEFAULT;
		this.stored = FieldInfo.STORED_DEFAULT;
		this.tokenized = FieldInfo.TOKENIZED_DEFAULT;
	}

	public FieldInfo(boolean indexed, boolean stored, Class parser)  {
		this.indexed = indexed;
		this.stored = stored;
		if (parser.isAssignableFrom(TextParser.class)) {
			this.parser = parser;
			this.tokenized = true;
		} else {
			this.tokenized = false;
		}
	}

	public FieldInfo(boolean indexed, boolean stored) {
		this.indexed = indexed;
		this.stored = stored;
		this.tokenized = false;
	}

	public boolean isIndexed() {
		return this.indexed;
	}

	public boolean isStored() {
		return this.stored;
	}

	public boolean isTokenized() {
		return this.tokenized;
	}

	public Class getParser() {
		return parser;
	}
}
