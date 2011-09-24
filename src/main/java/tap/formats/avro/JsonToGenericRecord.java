package tap.formats.avro;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToGenericRecord {
  private static ObjectMapper m = new ObjectMapper();
  private static final Logger LOG = LoggerFactory
      .getLogger(JsonToGenericRecord.class);

  public static GenericContainer jsonToRecord(String line, Schema schema)
      throws IOException {
    JsonNode rootNode = m.readTree(line);

    if (rootNode.isArray()) {
      GenericArray<GenericRecord> arr = new GenericData.Array<GenericRecord>(
          rootNode.size(), schema);
      for (int i = 0; i < rootNode.size(); i++) {
        arr.add(recordFromNode(rootNode.get(i), schema, line));
      }
      return arr;
    } else if (rootNode.isObject()) {
      GenericRecord r = recordFromNode(rootNode, schema, line);
      return r;
    } else {
      LOG.debug("no container?");
    }
    return null;
  }

  private static Map<Schema, Set<String>> errors = new HashMap<Schema, Set<String>>();

  private static GenericRecord recordFromNode(JsonNode node, Schema schema,
      String container) {
    schema = getObject(schema);
    GenericRecord r = new GenericData.Record(schema);
    Iterator<JsonNode> it = node.getElements();
    Iterator<String> itn = node.getFieldNames();
    while (it.hasNext()) {
      JsonNode child = it.next();
      String name = replaceInvalidNameChars(itn.next());
      Field field = schema.getField(name);
      if (field == null) {
        if (addToErrors(schema, name))
          LOG.warn("skipping unmapped field " + name + " contained in "
              + container);
        continue;
      }
      Schema childSchema = field.schema();
      if (child.isArray()) {
        GenericArray<Object> o = recordFromArrayNode(child, childSchema, name);
        r.put(name, o);

      } else if (child.isObject()) {
        GenericRecord o = recordFromNode(child, childSchema, name);
        r.put(name, o);
      } else if (child.isInt()) {
        if (acceptsInt(childSchema)) {
          r.put(name, child.getIntValue());
        } else if (acceptsLong(childSchema)) {
          r.put(name, (long) child.getIntValue());
        } else if (acceptsFloat(childSchema)) {
          r.put(name, (float) child.getIntValue());
        } else if (acceptsDouble(childSchema)) {
          r.put(name, (double) child.getIntValue());
        } else {
          if (addToErrors(schema, name))
            LOG.error("Can't store an int in field " + name);
        }
      } else if (child.isLong()) {
        if (acceptsLong(childSchema)) {
          r.put(name, (long) child.getLongValue());
        } else if (acceptsFloat(childSchema)) {
          r.put(name, (float) child.getLongValue());
        } else if (acceptsDouble(childSchema)) {
          r.put(name, (double) child.getLongValue());
        } else {
          if (addToErrors(schema, name))
            LOG.error("Can't store a long in field " + name);
        }
      } else if (child.isBinary()) {
        try {
          r.put(name, child.getBinaryValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (child.isBoolean()) {
        r.put(name, child.getBooleanValue());
      } else if (child.isDouble()) {
        if (acceptsDouble(childSchema)) {
          r.put(name, child.getDoubleValue());
        } else if (acceptsFloat(childSchema)) {
          r.put(name, (float) child.getDoubleValue());
        } else {
          if (addToErrors(schema, name))
            LOG.error("Can't store a double in field " + name);
        }
      } else if (child.isTextual()) {
        r.put(name, child.getTextValue()); // new
                                           // org.apache.avro.util.Utf8(child.getTextValue()));
      }
    }
    return r;
  }

  private static boolean addToErrors(Schema schema, String name) {
    Set<String> err = errors.get(schema);
    if (err == null) {
      err = new TreeSet<String>();
      errors.put(schema, err);
    }
    return err.add(name);
  }

  private static GenericArray<Object> recordFromArrayNode(JsonNode node,
      Schema schema, String container) {
    schema = getArray(schema);
    GenericArray<Object> r = new GenericData.Array<Object>(node.size(), schema);
    Schema childSchema = schema.getElementType();
    for (int i = 0; i < node.size(); i++) {
      JsonNode child = node.get(i);
      if (child.isArray()) {
        GenericArray<Object> o = recordFromArrayNode(child, childSchema,
            container);
        r.add(o);
      } else if (child.isObject()) {
        GenericRecord o = recordFromNode(child, childSchema, container);
        r.add(o);
      } else if (child.isInt()) {
        if (acceptsInt(childSchema)) {
          r.add(child.getIntValue());
        } else if (acceptsLong(childSchema)) {
          r.add((long) child.getIntValue());
        } else if (acceptsFloat(childSchema)) {
          r.add((float) child.getIntValue());
        } else if (acceptsDouble(childSchema)) {
          r.add((double) child.getIntValue());
        } else {
          LOG.error("Can't store an int in field " + childSchema);
        }
      } else if (child.isLong()) {
        if (acceptsLong(childSchema)) {
          r.add((long) child.getLongValue());
        } else if (acceptsFloat(childSchema)) {
          r.add((float) child.getLongValue());
        } else if (acceptsDouble(childSchema)) {
          r.add((double) child.getLongValue());
        } else {
          LOG.error("Can't store a long in field " + childSchema);
        }
      } else if (child.isBinary()) {
        try {
          r.add(child.getBinaryValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (child.isBoolean()) {
        r.add(child.getBooleanValue());
      } else if (child.isDouble()) {
        if (acceptsDouble(childSchema)) {
          r.add(child.getDoubleValue());
        } else if (acceptsFloat(childSchema)) {
          r.add((float) child.getDoubleValue());
        } else {
          LOG.error("Can't store a double in field " + childSchema);
        }
      } else if (child.isTextual()) {
        r.add(new org.apache.avro.util.Utf8(child.getTextValue()));
      }
    }
    return r;
  }

  private static Schema getObject(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD)
      return schema;
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema s : schema.getTypes()) {
        if (s.getType() == Schema.Type.RECORD) {
          return s;
        }
      }
    }
    return null;
  }

  private static Schema getArray(Schema schema) {
    if (schema.getType() == Schema.Type.ARRAY)
      return schema;
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema s : schema.getTypes()) {
        if (s.getType() == Schema.Type.ARRAY) {
          return s;
        }
      }
    }
    return null;
  }

  private static boolean accepts(Schema childSchema, Schema.Type type) {
    if (childSchema.getType() == type)
      return true;
    if (childSchema.getType() == Schema.Type.UNION) {
      for (Schema s : childSchema.getTypes()) {
        if (s.getType() == type) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean acceptsFloat(Schema childSchema) {
    return accepts(childSchema, Schema.Type.FLOAT);
  }

  private static boolean acceptsDouble(Schema childSchema) {
    return accepts(childSchema, Schema.Type.DOUBLE);
  }

  private static boolean acceptsLong(Schema childSchema) {
    return accepts(childSchema, Schema.Type.LONG);
  }

  private static boolean acceptsInt(Schema childSchema) {
    return accepts(childSchema, Schema.Type.INT);
  }

  // hand-written for speed - regexps are 100x slower
  static String replaceInvalidNameChars(String name) {
    int len = name.length();
    // scan for first invalid char
    int i = 0;
    for (; i < len; i++) {
      char ch = name.charAt(i);
      if (!(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0'
          && ch <= '9' || ch == '_'))
        break;
    }
    if (i == len)
      return name;
    StringBuilder copy = new StringBuilder(len);
    copy.append(name, 0, i);
    copy.append('_');
    i++;
    for (; i < len; i++) {
      char ch = name.charAt(i);
      if (ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0'
          && ch <= '9' || ch == '_') {
        copy.append(ch);
      } else {
        copy.append('_');
      }
    }

    return copy.toString();
  }
}
