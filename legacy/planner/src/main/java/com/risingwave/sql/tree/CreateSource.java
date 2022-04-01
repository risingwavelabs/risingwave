package com.risingwave.sql.tree;

import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/** CreateSource is a node that represents the CREATE SOURCE statement. */
public final class CreateSource extends Statement {
  private final String name;
  private final List<Node> tableElements;
  private final GenericProperties<Expression> properties;
  private final String rowFormat;
  private final String rowSchemaLocation;

  public CreateSource(
      String streamName,
      List<Node> tableElements,
      GenericProperties<Expression> props,
      String rowFormat,
      String rowSchemaLocation) {
    this.name = streamName;
    this.tableElements = tableElements;
    this.properties = props;
    this.rowFormat = rowFormat;
    this.rowSchemaLocation = rowSchemaLocation;
  }

  public String getName() {
    return name;
  }

  public List<Node> getTableElements() {
    return tableElements;
  }

  public GenericProperties<Expression> getProperties() {
    return properties;
  }

  public String getRowFormat() {
    return rowFormat;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateSource(this, context);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(name)
        .append(tableElements)
        .append(properties)
        .append(rowFormat)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateSource rhs = (CreateSource) o;
    return new EqualsBuilder()
        .append(name, rhs.name)
        .append(tableElements, rhs.tableElements)
        .append(properties, rhs.properties)
        .append(rowFormat, rhs.rowFormat)
        .isEquals();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append(name)
        .append(tableElements)
        .append(properties)
        .append(rowFormat)
        .build();
  }

  public String getRowSchemaLocation() {
    return rowSchemaLocation;
  }
}
