package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class SchemaAliasTest {
/*
  @Test
  public void aliasTest() throws JsonProcessingException {
    Schema schema = Schema.createRecord(name, doc, nameSpace, isError);

    for(String alias : aliasList) {
      schema.addAlias(alias);
    }

    //Check if correctly created in schema
    String schemaJson = schema.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(schemaJson);

    JsonNode aliasesJson = actualObj.get("aliases");
    for(int i = 0; i < aliasesJson.size(); i++) {
      Assert.assertEquals(aliasList.get(i), aliasesJson.get(i).asText());
    }


    //Schema.applyAliases
  }*/
}
