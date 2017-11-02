/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import java.nio.ByteBuffer;

public class SqlPatternContainsMatcher extends AbstractSqlPatternMatcher {

  public SqlPatternContainsMatcher(String patternString) {
    super(patternString);
  }

  @Override
  public int match(int start, int end, DrillBuf drillBuf) {
    int patternIndex = 0;
    int txtIndex = 0;

   // ByteBuffer inputByteBuf = drillBuf.nioBuffer(start, end - start);
    int txtLength = end - start;

    if (txtIndex < patternIndex) {
      return 0;
    }

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    while (patternIndex < patternLength && txtIndex < txtLength) {
      if (patternByteBuffer.get(patternIndex) != drillBuf.getByte(start + txtIndex)) {
        // Go back if there is no match
        txtIndex = txtIndex - patternIndex;
        patternIndex = 0;
      } else {
        patternIndex++;
      }
      txtIndex++;
    }

    return (patternIndex == patternLength) ? 1 : 0;
  }

}
