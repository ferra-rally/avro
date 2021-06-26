package org.apache.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BinaryDecoderTest {
  private Schema schema;

  @Before
  public void config() {
    schema = SchemaBuilder.record("User")
      .namespace("test.avro")
      .fields()
      .endRecord();

  }

  @Test
  public void test() throws IOException {
    /*
    User user1 = new User();
    user1.setName("Alyssa");
    user1.setNumber(256);

    User user2 = new User("Ben", 7, "red");

    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
    dataFileWriter.create(schema, new File("users.avro"));

    dataFileWriter.append(user1);
    dataFileWriter.append(user2);

    */


  }

}
