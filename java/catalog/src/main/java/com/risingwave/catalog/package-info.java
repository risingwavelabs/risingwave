/**
 * Contains catalog service implementation and catalog related classes
 *
 * <p>Though in current implementation catalog entries, e.g. database, schema, table, are mutable,
 * all mutation method should be <b>package private</b>, and should look immutable to users outside
 * this package. {@link com.risingwave.catalog.CatalogService} should be the only interface to
 * manipulate these entries.
 */
package com.risingwave.catalog;
