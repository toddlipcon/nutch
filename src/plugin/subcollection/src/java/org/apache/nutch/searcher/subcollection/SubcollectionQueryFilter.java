/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.searcher.subcollection;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.subcollection.SubcollectionIndexingFilter;
import org.apache.nutch.searcher.RawFieldQueryFilter;

/** Handles "collection:" query clauses, causing them to search the "collection" field
 * indexed by SubcollectionINdexingFilter. */
public class SubcollectionQueryFilter extends RawFieldQueryFilter {
  public SubcollectionQueryFilter() {
    super(SubcollectionIndexingFilter.FIELD_NAME);
  }

  public void setConf(Configuration conf) {
    // nothing to configure
  }

  public Configuration getConf() {
    // nothing configured
    return null;
  }
}
