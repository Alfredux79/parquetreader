package io.alfredux.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.util.logging.Logger;


public class ParquetFileReader {

    private static Logger logger = Logger.getLogger(ParquetFileReader.class.getSimpleName());

    public static void main(String ... args){

        Path file = new Path("/opt/apache-drill-1.13.0/sample-data/nation.parquet");
        System.out.println(file.getName());
        try {
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
            System.out.println(reader.read());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
