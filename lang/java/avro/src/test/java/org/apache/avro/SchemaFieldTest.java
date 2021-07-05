package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.example.SampleClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

//@RunWith(value = Parameterized.class)
public class SchemaFieldTest {
  /*
  private Schema schema;
  private String name;
  private Schema fieldType;
  private String doc;
  private Object defaultValue;

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {"name", SchemaBuilder.builder().intType(), "doc", 3}
    });
  }

  public SchemaFieldTest(String name, Schema fieldType, String doc, Object defaultValue) {
    this.name = name;
    this.fieldType = fieldType;
    this.doc = doc;
    this.defaultValue = defaultValue;
  }

  @Before
  public void config() {
    schema = Schema.createRecord("test_schema", "", "test", false);
  }

  @Test
  public void createFieldTest() {
    Schema.Field field = new Schema.Field(name, fieldType, doc, defaultValue);
    Assert.assertNotNull(field);

    //Schema not assigned has -1 position
    Assert.assertEquals(-1, field.pos());

    List<Schema.Field> fieldList = new ArrayList<>();

    fieldList.add(field);

    schema.setFields(fieldList);

    //Check if is in the first position
    Assert.assertEquals(0, field.pos());
  }*/

  /*
  @Test
  public void schemaCreateRecordTest() throws JsonProcessingException {
    Schema schema = Schema.createRecord(name, doc, nameSpace, isError);

    schema.setFields(fields);

    for(Schema.Field field : fields) {
      Schema.Field out = schema.getField(field.name());

      Assert.assertEquals(field, out);
    }

    //Check if correctly created in schema
    String schemaJson = schema.toString();
    System.out.println(schemaJson);
    ObjectMapper mapper = new ObjectMapper();

    JsonNode actualObj = mapper.readTree(schemaJson);
    Assert.assertEquals(name, actualObj.get("name").asText());
    Assert.assertEquals(nameSpace, actualObj.get("namespace").asText());
    Assert.assertEquals(doc, actualObj.get("doc").asText());

    JsonNode fieldJson = actualObj.get("fields");
    for(int i = 0; i < fields.size(); i++) {
      System.out.println(fields.);
      Assert.assertEquals(fields.get(i), fieldJson.get(i));
    }
  }*/
}
