package com.risingwave.catalog;

import java.util.Collection;

public class DatabaseCatalog extends AbstractNonLeafEntity<SchemaCatalog> {
  public DatabaseCatalog(int id, String name, Collection<SchemaCatalog> schemas) {
    super(id, name, schemas);
  }
}
