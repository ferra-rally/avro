package org.apache.avro;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.example.SampleClass;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

@RunWith(value = Parameterized.class)
public class SchemaTest {
  private String name;
  private List<String> aliasList;
  private List<String> enumFields;
  private boolean isError;
  private List<Schema.Field> fields;

  public SchemaTest(String name, List<String> aliasList, List<String> enumFields, boolean isError, List<Schema.Field> fields) {
    this.name = name;
    this.aliasList = aliasList;
    this.enumFields = enumFields;
    this.isError = isError;
    this.fields = fields;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {"test_name", new ArrayList<String>() {{add("alias1"); add("alias2");}}, new ArrayList<String>() {{  add("A"); add("B");}}, false,
      new ArrayList<Schema.Field>() {{add(new Schema.Field("a", SchemaBuilder.builder().intType(), "", 2));}}}
    });
  }

  @Before
  public void config() {

  }

  @Test
  public void schemaEnumTest() {

    Schema schema = Schema.createEnum(name, "", "", enumFields);

    Assert.assertEquals(enumFields, schema.getEnumSymbols());
  }

  @Test
  public void aliasTest() {
    Schema schema =  Schema.createEnum(name, "", "", enumFields);

    for(String alias : aliasList) {
      schema.addAlias(alias);
    }

    Set<String> aliases = schema.getAliases();

    for(String name : aliases) {
      boolean contains = aliasList.contains(name);
      Assert.assertTrue(contains);
    }
  }

  @Test
  public void schemaCreateRecordTest() {
    List<Schema.Field> fields = new ArrayList<>();

    Schema schema = Schema.createRecord(name, "", "", isError);

    schema.setFields(fields);

    for(Schema.Field field : fields) {
      Schema.Field out = schema.getField(field.name());

      Assert.assertEquals(field, out);
    }
  }
}
