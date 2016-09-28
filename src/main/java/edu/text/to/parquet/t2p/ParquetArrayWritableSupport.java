package edu.text.to.parquet.t2p;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter;
import org.apache.hadoop.io.ArrayWritable;

import parquet.hadoop.api.WriteSupport;

import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class ParquetArrayWritableSupport extends WriteSupport<ArrayWritable> {

	private DataWritableWriter writer = null;

	private RecordConsumer consumer = null;
	private MessageType schema = null;

	public static final String SCHEMA_KEY = "parquet.parquetarraywritesupport.schema";

	public static void setSchema(Configuration config, MessageType schema) {
		config.set(ParquetArrayWritableSupport.SCHEMA_KEY, schema.toString());
	}

	public static MessageType getSchema(Configuration configuration) {
		return MessageTypeParser.parseMessageType(configuration.get(ParquetArrayWritableSupport.SCHEMA_KEY));
	}

	public ParquetArrayWritableSupport() {
	}

	@Override
	public WriteContext init(Configuration config) {
		String schema = config.get(SCHEMA_KEY);

		if (schema == null || schema.trim().equalsIgnoreCase(""))
			throw new IllegalStateException("Schema cannot be null or empty");
		else
			this.schema = ParquetArrayWritableSupport.getSchema(config);

		return new WriteContext(this.schema, new HashMap<String, String>());
	}

	@Override
	public void prepareForWrite(RecordConsumer recordConsumer) {
		this.consumer = recordConsumer;
	}

	@Override
	public void write(ArrayWritable data) {
		// sweet double checked locking
		if (writer == null) {
			if (this.consumer == null)
				throw new IllegalStateException("RecordConsumer cannot be null");
			if (this.schema == null)
				throw new IllegalStateException("Schema cannot be null");

			synchronized (this) {
				if (this.writer == null)
					this.writer = new DataWritableWriter(this.consumer, this.schema);
			}
		}

		this.writer.write(data);
	}

}
