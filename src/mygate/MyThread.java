/*
 * Implementation of MyThread class, required to process annotations in
 *  a concurrent way.
 * Author: Jose Figueredo
 * Last modification: 09/08/2015
 */

package mygate;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.client.MongoCollection;

import gate.Annotation;
import gate.Factory;
import gate.FeatureMap;

class MyThread implements Runnable {
  List<Annotation> list;
  Hashtable<Long, Annotation> table;
  MongoCollection<org.bson.Document> collection;

  // Mythread constructor
  MyThread(List<Annotation> l, Hashtable<Long, Annotation> t, MongoCollection<org.bson.Document> c) {
    list = l;
    table = t;
    collection = c;
    System.out.println("Creating thread");
  }

  // Mythread associated method
  public void run() {
    Iterator<Annotation> it = list.iterator();
    while (it.hasNext()) {
      Annotation a = it.next();
      process(a);
    }
    System.out.println("Thread exiting.");
  }

  // method to get label and features of an annotation
  public void process(Annotation o) {
    String inst = (String) o.getFeatures().get("inst");
    // setting NCBITaxon term features
    if (inst.equals("urn:organism_from_ncbi")) {
      // setting term features
      FeatureMap annotFeatures = Factory.newFeatureMap();
      annotFeatures.put("majorType", "organism");
      annotFeatures.put("minorType", "organism_from_ncbi");
      annotFeatures.put("language", "en");
      // getting found label
      long start = o.getStartNode().getOffset();
      long end = o.getEndNode().getOffset();
      String label = getLabel("", start, end, table, 0);
      // iterating over features of the found label to set them
      @SuppressWarnings("unchecked")
      Map<String, String> features = (Map<String, String>) getFeatures(label, collection);
      Iterator<Entry<String, String>> iterator = features.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, String> entry = iterator.next();
        String key = entry.getKey();
        String value = entry.getValue();
        annotFeatures.put(key, value);
      }
      // setting term features
      o.setFeatures(annotFeatures);
    }
  }

  /* 
   * method to get string label from its indexes
   * @s accumulated string, useful to avoid tail recursion 
   * @start label start index
   * @end label end index
   * @table hashtable with tokens annotations
   * @i number of "left chars" to delete from current string
   */
  public static String getLabel(String s, long start, long end, Hashtable<Long, Annotation> table, int i) {
    // getting first token
    Annotation a = table.get(start);
    String label;
    // if no accumulated chars
    if (s == "") {
      // if nothing found, there is not a token with the same start, but there must be one with same end
      // so, searching again adding a "left char" to delete
      try {
        boolean isNull = a.getFeatures().get("string") == null;
      } catch (Exception e) {
        return getLabel(s, start - 1, end, table, i + 1);
      }
      // if something found, get string
      label = a.getFeatures().get("string").toString();
    } else {
      // if nothing found with not empty @s, term must have more than one token
      // and there must be a middle char like a space or a tab
      // so, searching again skipping a char
      if (a == null) {
        return getLabel(s, start + 1, end, table, i);
      }
      // if something found, updating label and adding space between tokens
      label = s + " " + a.getFeatures().get("string").toString();
    }
    // getting end index from current token annotation
    long e = a.getEndNode().getOffset();
    // if token is longer than label
    if (e > end) {
      int dif = (int) (e - end);
      // safe cast from long to int
      if ((long) dif != (e - end)) {
        throw new IllegalArgumentException();
      }
      // cutting label string according to label
      label = label.substring(0, label.length() - dif).toString();
    }
    // if token is shorter than label, there are more tokens to get
    if (e < end) {
      // checking if next token to get is right there or there is a middle space 
      if (table.get(e) != null) {
        return label + getLabel("", e, end, table, i);
      } else {
        return getLabel(label, e + 1, end, table, i);
      }
    }
    // returning label skipping "left chars" to delete
    return label.substring(i, label.length());
  }

  // method to get features of a term from a collection in MongoDB
  private static Object getFeatures(String label, MongoCollection<org.bson.Document> collection) {
    // setting query
    org.bson.Document doc = new org.bson.Document("label", label.toLowerCase());
    // getting features
    org.bson.Document term = collection.find(doc).first();
    return term.get("features");
  }

}