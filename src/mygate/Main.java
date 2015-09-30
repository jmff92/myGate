/*
 * This code creates a document from an URL, and add it to a corpus to be 
 * proccesed by a pipeline on GATE application, getting a XML document as
 * output. The pipeline is built with some PRs and customized gazetteers.
 * Author: Jose Figueredo
 * Last modification: 09/08/2015
 */

package mygate;

import gate.*;
import gate.creole.ResourceInstantiationException;
import gate.creole.SerialAnalyserController;
import gate.gui.*;
import gate.util.GateException;
import gate.util.InvalidOffsetException;

import javax.swing.SwingUtilities;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Main {

  // filepath
//  static String root = "C:/Users/jfigueredo/Desktop/mygate/";
  static String root = System.getProperty("user.dir")+"/";

  public static void main(String[] args) throws Exception {

    startGate();

    // Creating document and corpus
//    Document doc =
    // newDocument("file:/"+root+"input/corpus_of_protocols/Rare allele
    // enrichment and detection by allele-specific PCR, competitive probe
    // blocking and melting analysis.pdf", "UTF-8");
//    newDocument("file:/" + root + "input/test.txt", "UTF-8");
    // newDocument("file:/"+root+"input/Practical_DGS_protocfor large-scale
    // genome structural study.pdf","UTF-8");
    // newDocument("file:/"+root+"input/Protocol Exchange_4.pdf","UTF-8");
    Corpus corpus = Factory.newCorpus("Corpus");
//    corpus.add(doc);

    // Creating pipeline
    SerialAnalyserController controller = (SerialAnalyserController) Factory
        .createResource("gate.creole.SerialAnalyserController");
    loadPRs(controller);
    // Setting controller corpus
    controller.setCorpus(corpus);
    // Executing pipeline
    controller.execute();

    // getting annotations
//    AnnotationSet annot = doc.getAnnotations();

    // processing annotations (printing)
//    processAnnots(annot);

    // creating xml file with annotations
//    createXML(doc);
  }

  // method to start GATE and show its main window
  private static void startGate() throws GateException, InterruptedException, InvocationTargetException {
    // initializing gate
    org.apache.log4j.BasicConfigurator.configure();
    Gate.init();
    // showing the main window
    SwingUtilities.invokeAndWait(new Runnable() {
      public void run() {
        MainFrame.getInstance().setVisible(true);
      }
    });
  }

  // method to create a new document with an url and an encoding
  private static Document newDocument(String url, String encoding)
      throws MalformedURLException, ResourceInstantiationException {
    // source and encoding
    FeatureMap params = Factory.newFeatureMap();
    params.put(Document.DOCUMENT_URL_PARAMETER_NAME, new URL(url));
    params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
    FeatureMap feats = Factory.newFeatureMap();
    // creating document
    return (Document) Factory.createResource("gate.corpora.DocumentImpl", params, feats, "Document");
  }

  // method to load PRs on pipeline
  private static void loadPRs(SerialAnalyserController controller) throws MalformedURLException, GateException {
    // get the root plugins dir
    File pluginsDir = Gate.getPluginsHome();
    // load the ANNIE plugin
    File aPluginDir = new File(pluginsDir, "ANNIE");
    // loading plugins
    Gate.getCreoleRegister().registerDirectories(aPluginDir.toURI().toURL());
    File ncbiPluginDir = new File(pluginsDir, "NCBITagger");
    Gate.getCreoleRegister().registerDirectories(ncbiPluginDir.toURI().toURL());    
    File chebiPluginDir = new File(pluginsDir, "CHEBITagger");
    Gate.getCreoleRegister().registerDirectories(chebiPluginDir.toURI().toURL());
    
    // Plugins: https://gate.ac.uk/gate/doc/plugins.html
    // Document PR
    controller = loadPR("gate.creole.annotdelete.AnnotationDeletePR", controller);
    // Tokeniser
    controller = loadPR("gate.creole.tokeniser.DefaultTokeniser", controller);
    // Sentence Splitter
    controller = loadPR("gate.creole.splitter.SentenceSplitter", controller);
    // POS Tagger
    controller = loadPR("gate.creole.POSTagger", controller);
    // Morphological analyser
//    controller = loadPR("gate.creole.morph.Morph", controller);
    // Flexible gazetteer
    System.out.println("file:/" + root + "gazetteers/simple/simple.def");
    controller = loadFlexGaz("$", "file:/" + root + "gazetteers/simple/simple.def", controller);
    // Large KB Gazetteer (NCBITaxon and CHEBI terms)
    controller = loadLKBG("file:/" + root + "gazetteers/large/NCBITaxon", controller, "Organism");
    controller = loadLKBG("file:/" + root + "gazetteers/large/CHEBI", controller, "Reagent");
    // JAPE Rules
    controller = loadRules(root + "rules/", controller);
    // NCBI tagger
    controller = loadPR("NCBITagger.Main", controller);
    // CHEBI tagger    
    controller = loadPR("CHEBITagger.Main", controller);
  }

  // method to load a PR from its name
  private static SerialAnalyserController loadPR(String prName, SerialAnalyserController controller)
      throws ResourceInstantiationException {
    ProcessingResource pr = (ProcessingResource) Factory.createResource(prName);
    controller.add(pr);
    return controller;
  }

  // method to load a flexible gazetteer
  private static SerialAnalyserController loadFlexGaz(String separator, String url, SerialAnalyserController controller)
      throws ResourceInstantiationException, MalformedURLException {
    // Default Gazetteer
    FeatureMap gztFeatures = Factory.newFeatureMap();
    gztFeatures.put("caseSensitive", false);
    gztFeatures.put("encoding", "UTF-8");
    gztFeatures.put("gazetteerFeatureSeparator", separator);
    gztFeatures.put("listsURL", new URL(url));
    ProcessingResource gzt = (ProcessingResource) Factory.createResource("gate.creole.gazetteer.DefaultGazetteer",
        gztFeatures);
    // Flexible gazetteer
    FeatureMap fgzFeatures = Factory.newFeatureMap();
    fgzFeatures.put("gazetteerInst", gzt);
    fgzFeatures.put("inputFeatureNames", Arrays.asList("Token.root"));
    ProcessingResource fgz = (ProcessingResource) Factory.createResource("gate.creole.gazetteer.FlexibleGazetteer",
        fgzFeatures);
    controller.add(fgz);
    return controller;
  }

  // method to load a large KB gazetteer
  private static SerialAnalyserController loadLKBG(String url, SerialAnalyserController controller, String setname)
      throws ResourceInstantiationException, MalformedURLException {
    FeatureMap lkgFeatures = Factory.newFeatureMap();
    lkgFeatures.put("dictionaryPath", new URL(url));
    // setting annotation set name
    lkgFeatures.put("annotationSetName", setname);
    ProcessingResource lkg = (ProcessingResource) Factory.createResource("com.ontotext.kim.gate.KimGazetteer",
        lkgFeatures);
    controller.add(lkg);
    return controller;
  }

  // method to load JAPE rules from a folder in alphabetical order
  private static SerialAnalyserController loadRules(String path, SerialAnalyserController controller)
      throws ResourceInstantiationException {
    File dir = new File(path);
    File[] directoryListing = dir.listFiles();
    for (File child : directoryListing) {
      FeatureMap jptFeatures = Factory.newFeatureMap();
      jptFeatures.put("name", child.getName().split("\\.")[0]);
      System.out.println("Loading rule: " + child.getName());
      jptFeatures.put("grammarURL", "file:/" + path + child.getName());
      jptFeatures.put("encoding", "UTF-8");
      ProcessingResource jpt = (ProcessingResource) Factory.createResource("gate.creole.Transducer", jptFeatures);
      controller.add(jpt);
    }
    return controller;
  }

  // method to process annotations (printing them)
  private static void processAnnots(AnnotationSet annots) {
    Set<String> annotTypes = annots.getAllTypes();
    // iterating over annotation types
    for (String aType : annotTypes) {
      // getting number of annotations per type
      AnnotationSet annotByType = annots.get(aType);
      System.out.println("\nNumber of annotations for " + aType + ": " + annotByType.size());
      // iterating over annotations
      for (Annotation a : annotByType) {
        // getting annotation type
        String type = (String) a.getType();
        // term features
        long start = a.getStartNode().getOffset();
        long end = a.getEndNode().getOffset();
        int id = a.getId();
        String features = a.getFeatures().toString();
        System.out.println(type + " | " + start + " | " + end + " | " + id + " | " + features);
      }
    }
  }

  // method to create XML with annotations from input
  public static void createXML(Document doc) throws IOException {
    String output = root + "output/doc.xml";
    FileWriter writer = new FileWriter(output, false);
    String xml = doc.toXml();
    writer.write(xml);
    writer.flush();
    writer.close();
  }

}
