package org.apache.avro;

import org.apache.avro.example.SampleClass;
import org.junit.Test;

import java.util.ArrayList;

public class SchemaTest {

  @Test
  public void schemaEnumTest() {

    Schema schema = Schema.createEnum("enum", "", "", new ArrayList<String>() {{  add("A"); add("B");}});
    System.out.println(schema.getEnumSymbols());
  }
}
