package org.apache.avro;

import org.apache.avro.example.SampleClass;
import org.junit.Test;

public class ProtocolTest {

  @Test
  public void test() {
    Protocol protocol = new Protocol("test_protocol", "test.protocol", "Testing protocol");
/*
    Protocol.Message message = new Protocol.Message();
    protocol.createMessage(, SampleClass.getClassSchema());*/
  }
}
