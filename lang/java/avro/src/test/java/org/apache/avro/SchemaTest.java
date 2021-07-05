package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(value = Parameterized.class)
public class SchemaTest {
  private String name;
  private boolean isError;
  private String doc;
  private List<Schema.Field> fields;
  private String nameSpace;

  public SchemaTest(String name,  boolean isError, String doc, String nameSpace, List<Schema.Field> fields) {
    this.name = name;
    this.isError = isError;
    this.doc = doc;
    this.nameSpace = nameSpace;
    this.fields = fields;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {"asd", false, "{version: 1}", null, new ArrayList<Schema.Field>() {{add(new Schema.Field("field", SchemaBuilder.builder().intType(), "asd", 3));}}},
      {"asd", false, "{version: 1}", null, new ArrayList<Schema.Field>() {{}}}
    });
  }

  @Before
  public void config() {

  }

  @Test
  public void schemaCreateRecordTest() throws JsonProcessingException {
    if(name.equals("")) {
      Assert.assertThrows(SchemaParseException.class, () -> {Schema.createRecord(name, doc, nameSpace, isError, fields);});
    } else {
      Schema schema = Schema.createRecord(name, doc, nameSpace, isError);

      //Check if correctly created in schema
      String schemaJson = schema.toString();
      ObjectMapper mapper = new ObjectMapper();

      JsonNode actualObj = mapper.readTree(schemaJson);

      Assert.assertEquals(name, actualObj.get("name").asText());
      if(nameSpace != null) {
        Assert.assertEquals(nameSpace, actualObj.get("namespace").asText());
      }
      Assert.assertEquals(doc, actualObj.get("doc").asText());
    }
  }

  @Test
  public void schemaCreateRecordWithFieldsTest() throws JsonProcessingException {

    if(name.equals("")) {
      Assert.assertThrows(SchemaParseException.class, () -> {Schema.createRecord(name, doc, nameSpace, isError, fields);});
    } else {
      Schema schema = Schema.createRecord(name, doc, nameSpace, isError, fields);

      //Check if correctly created in schema
      String schemaJson = schema.toString();

      ObjectMapper mapper = new ObjectMapper();

      JsonNode actualObj = mapper.readTree(schemaJson);

      Assert.assertEquals(name, actualObj.get("name").asText());
      if(nameSpace != null) {
        Assert.assertEquals(nameSpace, actualObj.get("namespace").asText());
      }
      Assert.assertEquals(doc, actualObj.get("doc").asText());

      JsonNode fieldsJson = actualObj.get("fields");
      for (int i = 0; i < fields.size(); i++) {
        JsonNode fieldJson = fieldsJson.get(i);
        Schema.Field field = fields.get(i);

        //Check if name and doc are present
        Assert.assertEquals(field.name(), fieldJson.get("name").asText());
        Assert.assertEquals(field.doc(), fieldJson.get("doc").asText());
      }
    }
  }
}
