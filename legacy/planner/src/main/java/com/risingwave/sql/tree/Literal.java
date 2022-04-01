/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package com.risingwave.sql.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Literal extends Expression {

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLiteral(this, context);
  }

  public static Literal fromObject(Object value) {
    Literal literal = null;
    if (value == null) {
      literal = NullLiteral.INSTANCE;
    } else if (value instanceof Number) {
      if (value instanceof Float || value instanceof Double) {
        literal = new DoubleLiteral(value.toString());
      } else if (value instanceof Short || value instanceof Integer) {
        literal = new IntegerLiteral(((Number) value).intValue());
      } else if (value instanceof Long) {
        literal = new LongLiteral((Long) value);
      }
    } else if (value instanceof Boolean) {
      literal = (Boolean) value ? BooleanLiteral.TRUE_LITERAL : BooleanLiteral.FALSE_LITERAL;
    } else if (value instanceof Object[]) {
      List<Expression> expressions = new ArrayList<>();
      for (Object o : (Object[]) value) {
        expressions.add(fromObject(o));
      }
      literal = new ArrayLiteral(expressions);
    } else if (value instanceof Map) {
      HashMap<String, Expression> map = new HashMap<>();
      @SuppressWarnings("unchecked")
      Map<String, Object> valueMap = (Map<String, Object>) value;
      for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
        map.put(entry.getKey(), fromObject(entry.getValue()));
      }
      literal = new ObjectLiteral(map);
    } else {
      literal = new StringLiteral(value.toString());
    }
    return literal;
  }
}
