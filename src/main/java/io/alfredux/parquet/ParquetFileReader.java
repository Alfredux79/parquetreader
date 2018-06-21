package io.alfredux.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetFileReader {

    public static void main(String... args) {

        Path file = new Path("src/main/resources/nation.parquet");
        System.out.println(file.toUri());
        try {
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
            GenericRecord result = reader.read();
            System.out.println(result.getSchema());
            while ((result = reader.read()) != null) {
                System.out.println(result);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
