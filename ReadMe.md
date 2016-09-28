#Text to Parquet 
This is a sample program which uses ParquetHiveSerde and hive DataWritableWriter to write text format data which is parsed and converted in a List<Object> and serialized by ParquetHiveSerde. DataWritableWriter is used in WriteSupport class to write the Writable data in parquet schema format. We need to have an established parquet schema . 

Remember ParquetHiveSerde support only ArrayWritable and hence we create a WriteSupport class with only ArrayWritable.


Running : 

yarn jar uber-t2p-0.0.1-SNAPSHOT.jar input_path junk_strings output_path

Currently the schema is a static data. 