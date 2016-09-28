package edu.text.to.parquet.t2p;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.Log;
import parquet.hadoop.ParquetOutputFormat;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class JobDriverParquet extends Configured implements Tool{
	private static final Log LOG = Log.getLog(JobDriverParquet.class);
	
	
	public static void main(String[] args) {
		
		try {
            int res = ToolRunner.run(new Configuration(), new JobDriverParquet(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
	}
	
	public static class ReadRequestMap extends Mapper<LongWritable, Text, NullWritable, Writable> {
		
		static String recordStruct = "struct<name:string,jobtitle:string>";
		static TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(recordStruct);
		static ObjectInspector oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
		SerDe serde = new ParquetHiveSerDe();
		
        @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	Writable wvalue = null;
        	String val= value.toString();
        	String[] vals = val.split(",", -1);
        	System.out.println(vals[0]+ "<<<+>>>" + vals[1]);
        	List<Object>  structObj = new ArrayList<Object>();
        	structObj.add(vals[0]);
        	structObj.add(vals[1]);
        	try {
        		wvalue = serde.serialize(structObj, oip);
			} catch (SerDeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    context.write(null, wvalue);
        }
    }

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.printf("Three parameters are required for Text to Parquet conversion- <input dir> <schema_path> <output dir>\n");
			return -1;
		}
		
		Job job = new Job(getConf());
		job.setJobName("Text to Parquet example");
		
		job.setJarByClass(JobDriverParquet.class);
		job.setMapperClass(ReadRequestMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		
		ParquetOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		ParquetOutputFormat.setOutputPath(job, new Path(args[2]));
		MessageType mt = MessageTypeParser.parseMessageType("message record { \n  optional binary name (UTF8); \n optional binary jobtitle (UTF8);\n } ");
		ParquetOutputFormat.setWriteSupportClass(job, ParquetArrayWritableSupport.class);
		ParquetArrayWritableSupport.setSchema(job.getConfiguration(), mt);
		LazyOutputFormat.setOutputFormatClass(job, ParquetOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.waitForCompletion(true);


		return 0;
	}

}
