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
import org.apache.drill.common.exceptions.DrillRuntimeException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public class SqlPatternContainsMatcher implements SqlPatternMatcher {
  final String patternString;
  final int patternLength;
  ByteBuffer patternByteBuffer;


  public SqlPatternContainsMatcher(String patternString) {
    this.patternString = patternString;

    CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
    CharBuffer patternCharBuffer = CharBuffer.wrap(patternString);
    try {
      this.patternByteBuffer = charsetEncoder.encode(patternCharBuffer);
    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error while encoding the pattern string " + patternString + " " + e);
    }

    this.patternLength = patternByteBuffer.limit();
  }

  @Override
  public int match(int start, int end, DrillBuf drillBuf) {
    int patternIndex = 0;
    int txtIndex = 0;

    ByteBuffer inputByteBuf;
    inputByteBuf = drillBuf.nioBuffer(start, end - start);
    int txtLength = inputByteBuf.limit();

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    while (patternIndex < patternLength && txtIndex < txtLength) {
      if (patternByteBuffer.get(patternIndex) != inputByteBuf.get(txtIndex)) {
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
