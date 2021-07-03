package org.apache.avro;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  private String doc;
  private String nameSpace;

  /*
  public SchemaTest(String name, List<String> aliasList, List<String> enumFields, boolean isError, List<Schema.Field> fields, String doc, String nameSpace) {
    this.name = name;
    this.aliasList = aliasList;
    this.enumFields = enumFields;
    this.isError = isError;
    this.fields = fields;
    this.doc = doc;
    this.nameSpace = nameSpace;
  }*/

  public SchemaTest(String name, List<String> aliasList, boolean isError, List<Schema.Field> fields, String doc, String nameSpace) {
    this.name = name;
    this.aliasList = aliasList;
    this.isError = isError;
    this.fields = fields;
    this.doc = doc;
    this.nameSpace = nameSpace;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      /*
      {"test_name", new ArrayList<String>() {{add("alias1"); add("alias2");}}, new ArrayList<String>() {{  add("A"); add("B");}}, false,
      new ArrayList<Schema.Field>() {{add(new Schema.Field("a", SchemaBuilder.builder().intType(), "", 2));}}, "doc", "test.avro"}*/
      {"test_name", new ArrayList<String>() {{add("alias1"); add("alias2");}}, false,
        new ArrayList<Schema.Field>() {{add(new Schema.Field("a", SchemaBuilder.builder().intType(), "", 2));}}, "doc", "test.avro"}
    });
  }

  @Before
  public void config() {

  }

  /*
  @Test
  public void schemaEnumTest() {

    Schema schema = Schema.createEnum(name, doc, nameSpace, enumFields);

    Assert.assertEquals(enumFields, schema.getEnumSymbols());
  }*/

  @Test
  public void aliasTest() throws JsonProcessingException {
    Schema schema = Schema.createRecord(name, doc, nameSpace, isError);

    for(String alias : aliasList) {
      schema.addAlias(alias);
    }

    /*
    Set<String> aliases = schema.getAliases();

    for(String name : aliases) {
      boolean contains = aliasList.contains(name);
      Assert.assertTrue(contains);
    }*/

    //Check if correctly created in schema
    String schemaJson = schema.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(schemaJson);

    JsonNode aliasesJson = actualObj.get("aliases");
    for(int i = 0; i < aliasesJson.size(); i++) {
      Assert.assertEquals(aliasList.get(i), aliasesJson.get(i).asText());
    }
  }

  @Test
  public void schemaCreateRecordTest() throws JsonProcessingException {
    Schema schema = Schema.createRecord(name, doc, nameSpace, isError);

    schema.setFields(fields);

    for(Schema.Field field : fields) {
      Schema.Field out = schema.getField(field.name());

      Assert.assertEquals(field, out);
    }

    //Check if correctly created in schema
    /*
    String schemaJson = schema.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(schemaJson);
    JsonNode fieldJson = actualObj.get("fields");

    for(int i = 0; i < fields.size(); i++) {
      Assert.assertEquals(fields.get(i), fieldJson.get(i));
    }*/
  }
}
