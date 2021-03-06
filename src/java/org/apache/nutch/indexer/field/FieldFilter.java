package org.apache.nutch.indexer.field;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.lucene.document.Document;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.plugin.Pluggable;

/**
 * Filter to manipulate FieldWritable objects for a given url during indexing.
 * 
 * Field filters are responsible for converting FieldWritable objects into 
 * lucene fields and adding those fields to the Lucene document.
 */
public interface FieldFilter
  extends Pluggable, Configurable {

  final static String X_POINT_ID = FieldFilter.class.getName();

  /**
   * Returns the document to which fields are being added or null if we are to
   * stop processing for this url and not add anything to the index.  All 
   * FieldWritable objects for a url are aggregated from databases passed into
   * the FieldIndexer and these fields are then passed into the Field filters.
   * 
   * It is therefore possible for fields to be added, removed, and changed 
   * before being indexed.
   * 
   * @param url The url to index.  
   * @param doc The lucene document
   * @param fields The list of FieldWritable objects representing fields for 
   * the index.
   * @return The lucene Document or null to stop processing and not index any
   * content for this url.
   * 
   * @throws IndexingException If an error occurs during indexing
   */
  public Document filter(String url, Document doc, List<FieldWritable> fields)
    throws IndexingException;

}
