package com.levandoski.invertedindex.document;

import com.levandoski.invertedindex.parse.TextParser;
import com.levandoski.invertedindex.parse.DataStream;
import com.levandoski.invertedindex.util.Logger;

/**
 * A Field belongs to a Document and is composed by a sequence of Terms. When the document is created
 * the Terms are Strings.
 */
public class Field {

	protected String name;
	protected String data;
	protected DataStream stream;

	private FieldInfo options;

	private static TextParser parser = null;

	public Field(final String name, String data, final FieldInfo options) {
		this.name = name;
		this.data = data;
		this.options = options;
	}

	public Field(String name, String data) {
		this(name, data, new FieldInfo());
	}

	public String name() {
		return name;
	}

	public String data() {
		return data;
	}

	public boolean isIndexed() {
		return options.isIndexed();
	}

	public boolean isStored() {
		return options.isStored();
	}

	public boolean isTokenized() {
		return options.isTokenized();
	}

	public void setData(String data) {
		this.data = data;
	}

	public TextParser getParser() {
		Class c = options.getParser();
		try {
			TextParser parser = (TextParser)c.newInstance();
			return parser;
		} catch (Exception e) {
			Logger.getInstance().error("couldn't create tokenizer object", e);
		}
		return null;
	}

	public DataStream getDataStream(TextParser parser) {
		if (!options.isIndexed()) {
			return null;
		}
		if (stream != null) {
			return stream;
		}
		return parser.dataStream(name, data);
	}
}
