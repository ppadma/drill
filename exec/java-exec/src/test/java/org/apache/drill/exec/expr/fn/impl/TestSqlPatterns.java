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
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSqlPatterns {

  @Test
  public void testSqlRegexLike() {
    // Given SQL like pattern, verify patternType is correct.
    // Java pattern should have % replaced with .*, _ replaced with .
    // Simple pattern should have meta (% and _) and escape characters removed.

    // A%B is complex
    RegexpUtil.SqlPatternInfo  patternInfo = RegexpUtil.sqlToRegexLike("A%B");
    assertEquals("A.*B", patternInfo.getJavaPatternString());
    assertEquals(RegexpUtil.SqlPatternType.COMPLEX, patternInfo.getPatternType());

    // A_B is complex
    patternInfo = RegexpUtil.sqlToRegexLike("A_B");
    assertEquals("A.B", patternInfo.getJavaPatternString());
    assertEquals(RegexpUtil.SqlPatternType.COMPLEX, patternInfo.getPatternType());

    // A%B%D is complex
    patternInfo = RegexpUtil.sqlToRegexLike("A%B%D");
    assertEquals("A.*B.*D", patternInfo.getJavaPatternString());
    assertEquals(RegexpUtil.SqlPatternType.COMPLEX, patternInfo.getPatternType());

    // %AB% is contains
    patternInfo = RegexpUtil.sqlToRegexLike("%AB%");
    assertEquals(".*AB.*", patternInfo.getJavaPatternString());
    assertEquals("AB", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.CONTAINS, patternInfo.getPatternType());

    // %AB is ends with
    patternInfo = RegexpUtil.sqlToRegexLike("%AB");
    assertEquals(".*AB", patternInfo.getJavaPatternString());
    assertEquals("AB", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.ENDS_WITH, patternInfo.getPatternType());

    // AB% is starts with
    patternInfo = RegexpUtil.sqlToRegexLike("AB%");
    assertEquals("AB.*", patternInfo.getJavaPatternString());
    assertEquals("AB", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.STARTS_WITH, patternInfo.getPatternType());

    // AB is constant.
    patternInfo = RegexpUtil.sqlToRegexLike("AB");
    assertEquals("AB", patternInfo.getJavaPatternString());
    assertEquals("AB", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.CONSTANT, patternInfo.getPatternType());

    // Test with escape characters.

    // A%#B is invalid escape sequence
    try {
      patternInfo = RegexpUtil.sqlToRegexLike("A%#B", '#');
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("Invalid escape sequence"));
    }

    // A#%B with # as escape character is constant A%B
    patternInfo = RegexpUtil.sqlToRegexLike("A#%B", '#');
    assertEquals("A%B", patternInfo.getJavaPatternString());
    assertEquals("A%B", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.CONSTANT, patternInfo.getPatternType());

    // %A#%B% is contains A%B
    patternInfo = RegexpUtil.sqlToRegexLike("%A#%B%", '#');
    assertEquals(".*A%B.*", patternInfo.getJavaPatternString());
    assertEquals("A%B", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.CONTAINS, patternInfo.getPatternType());

    // #%AB% is starts with %AB
    patternInfo = RegexpUtil.sqlToRegexLike("#%AB%", '#');
    assertEquals("%AB.*", patternInfo.getJavaPatternString());
    assertEquals("%AB", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.STARTS_WITH, patternInfo.getPatternType());

    // %#%AB#% is ends with %AB%
    patternInfo = RegexpUtil.sqlToRegexLike("%#%AB#%", '#');
    assertEquals(".*%AB%", patternInfo.getJavaPatternString());
    assertEquals("%AB%", patternInfo.getSimplePatternString());
    assertEquals(RegexpUtil.SqlPatternType.ENDS_WITH, patternInfo.getPatternType());

    // #_A#%B%C is complex
    patternInfo = RegexpUtil.sqlToRegexLike("#_A#%B%C", '#');
    assertEquals("_A%B.*C", patternInfo.getJavaPatternString());
    assertEquals(RegexpUtil.SqlPatternType.COMPLEX, patternInfo.getPatternType());

  }

  @Test
  public void testSqlPatternStartsWith() {
    try {
      RegexpUtil.SqlPatternInfo patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.STARTS_WITH, "", "ABC");
      SqlPatternMatcher sqlPatternStartsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      BufferAllocator allocator = RootAllocatorFactory.newRoot(16384);
      DrillBuf drillBuf = allocator.buffer(8192);
      CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
      CharBuffer charBuffer;
      ByteBuffer byteBuffer = null;

      charBuffer = CharBuffer.wrap("ABCD");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf));  // ABCD should match StartsWith ABC

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("BCD");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf));  // BCD should not match StartsWith ABC

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("XYZABC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf));  // XYZABC should not match StartsWith ABC

      // null text
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // null String should not match StartsWith ABC

      // pattern length > txt length
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("AB");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // AB should not match StartsWith ABC

      // startsWith null pattern should match anything
      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.STARTS_WITH,"", "");
      sqlPatternStartsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("AB");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // AB should match StartsWith null pattern

      // null pattern and null text
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // null text should match null pattern

      // wide character string.
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("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" +
        "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");

      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.STARTS_WITH,"",
        "b00dUrA0oa2i4ZEHg6zvPXPXlVQYB2BXe8T5gIEtvUDzcN6yUkIqyS07gaAy8k4");
      sqlPatternStartsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // should match

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.STARTS_WITH,"",
        "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");
      sqlPatternStartsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(0, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // should not match

      // non ascii
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("¤EÀsÆW°ê»Ú®i¶T¤¤¤ß3¼Ó®i¶TÆU2~~");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // should not match

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.STARTS_WITH,"", "¤EÀsÆW");
      sqlPatternStartsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("¤EÀsÆW");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternStartsWith.match(0, byteBuffer.limit(), drillBuf)); // should match

      drillBuf.close();
      allocator.close();

    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error while encoding the pattern string ", e);
    }
  }

  @Test
  public void testSqlPatternEndsWith() {
    try {
      RegexpUtil.SqlPatternInfo patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.ENDS_WITH, "", "BCD");
      SqlPatternMatcher sqlPatternEndsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      BufferAllocator allocator = RootAllocatorFactory.newRoot(16384);
      DrillBuf drillBuf = allocator.buffer(8192);
      CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
      CharBuffer charBuffer;
      ByteBuffer byteBuffer = null;

      charBuffer = CharBuffer.wrap("ABCD");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf));  // ABCD should match EndsWith BCD

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("ABC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf));  // ABC should not match EndsWith BCD

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf));  // null string should not match EndsWith BCD

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("A");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // ABCD should not match EndsWith A

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("XYZBCD");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf));  // XYZBCD should match EndsWith BCD

      // EndsWith null pattern should match anything
      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.ENDS_WITH, "", "");
      sqlPatternEndsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // AB should match StartsWith null pattern

      // null pattern and null text
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // null text should match null pattern

      // wide character string.
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("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" +
        "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");

      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.ENDS_WITH,"",
        "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");
      sqlPatternEndsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // should match

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.ENDS_WITH,"",
          "");
      sqlPatternEndsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // null text should match null pattern

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.ENDS_WITH,"",
        "atXzj4x3CgF6j1gn10aUJknF7KQLJ84D");
      sqlPatternEndsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(0, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // should not match

      // non ascii
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("¤EÀsÆW°ê»Ú®i¶T¤¤¤ß3¼Ó®i¶TÆU2~~");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // should not match

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.ENDS_WITH,"", "TÆU2~~");
      sqlPatternEndsWith = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternEndsWith.match(0, byteBuffer.limit(), drillBuf)); // should match

      drillBuf.close();
      allocator.close();

    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error while encoding the pattern string ", e);
    }

  }

  @Test
  public void testSqlPatternContains() {
    try {
      RegexpUtil.SqlPatternInfo patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONTAINS,".*ABC.*", "ABCD");
      SqlPatternMatcher sqlPatternContains = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      BufferAllocator allocator = RootAllocatorFactory.newRoot(16384);
      DrillBuf drillBuf = allocator.buffer(8192);
      CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
      CharBuffer charBuffer;
      ByteBuffer byteBuffer = null;

      charBuffer = CharBuffer.wrap("ABCD");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));  // ABCD should contain ABCD

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("BC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));  // BC cannot contain ABCD

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));  // null string should not match contains ABCD

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("DE");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));  // ABCD should not contain DE

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("xyzABCDqrs");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));   // xyzABCDqrs should contain ABCD

      // contains null pattern should match anything
      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONTAINS,"", "");
      sqlPatternContains = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("xyzABCDqrs");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf)); //  should match

      // null pattern and null text
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf)); // null text should match null pattern

      // wide character string.
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("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" +
        "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONTAINS,"",
        "tLsQ1gYWPF1geDalS5MdWfpDvF5MaeJMd2fK0m3jui7xY1IfuSxqZs7SEL6wUVGdWc5tsVroCMMy6Nqjdz5T4vW");
      sqlPatternContains = SqlPatternFactory.getSqlPatternMatcher(patternInfo);

      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONTAINS,"",
          "ABCDEF");
      sqlPatternContains = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(0, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf));

      // non ascii
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("¤EÀsÆW°ê»Ú®i¶T¤¤¤ß3¼Ó®i¶TÆU2~~");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf)); // should not match

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONTAINS,"", "¶T¤¤¤ß");
      sqlPatternContains = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternContains.match(0, byteBuffer.limit(), drillBuf)); // should match

      drillBuf.close();
      allocator.close();

    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error while encoding the pattern string ", e);
    }

  }

  @Test
  public void testSqlPatternConstant() {
    try {
      RegexpUtil.SqlPatternInfo patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONSTANT,"ABC.*", "ABC");
      SqlPatternMatcher sqlPatternConstant = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      BufferAllocator allocator = RootAllocatorFactory.newRoot(16384);
      DrillBuf drillBuf = allocator.buffer(8192);
      CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
      CharBuffer charBuffer;
      ByteBuffer byteBuffer = null;

      charBuffer = CharBuffer.wrap("ABC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf));  // ABC should match ABC

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("BC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf));  // ABC not same as BC

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf));  // null string not same as ABC

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("DE");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf));  // ABC not same as DE

      // null pattern should match null string
      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONSTANT,"", "");
      sqlPatternConstant = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf)); // null text should match null pattern

      // wide character string.
      StringBuffer sb = new StringBuffer("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" +
        "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");

      charBuffer = CharBuffer.wrap(sb);
      drillBuf.clear();
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONTAINS,"", sb.toString());
      sqlPatternConstant = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf));

    // non ascii
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("¤EÀsÆW°ê»Ú®i¶T¤¤¤ß3¼Ó®i¶TÆU2~~");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf));  // should not match

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.CONSTANT,"", "¤EÀsÆW°ê»Ú®i¶T¤¤¤ß3¼Ó®i¶TÆU2~~");
      sqlPatternConstant = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternConstant.match(0, byteBuffer.limit(), drillBuf)); // should match

      drillBuf.close();
      allocator.close();

    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error while encoding the pattern string ", e);
    }

  }

  @Test
  public void testSqlPatternComplex() {
    try {
      RegexpUtil.SqlPatternInfo patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.COMPLEX, "A.*BC.*", "");
      SqlPatternMatcher sqlPatternComplex = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      BufferAllocator allocator = RootAllocatorFactory.newRoot(16384);
      DrillBuf drillBuf = allocator.buffer(8192);
      CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
      CharBuffer charBuffer;
      ByteBuffer byteBuffer = null;

      charBuffer = CharBuffer.wrap("ABCDEF");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(1, sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf));  // ADEBCDF should match A.*BC.*

      drillBuf.clear();
      charBuffer = CharBuffer.wrap("BC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf));  // BC should not match A.*BC.*


      drillBuf.clear();
      charBuffer = CharBuffer.wrap("BC");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf), 0);  // null string should not match


      drillBuf.clear();
      charBuffer = CharBuffer.wrap("DEFGHIJ");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf), 0); // DEFGHIJ should not match A.*BC.*

      java.util.regex.Matcher matcher;
      matcher = java.util.regex.Pattern.compile("b00dUrA0.*").matcher("");

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.COMPLEX, "b00dUrA0.*42.*9a8BZ", "");


      drillBuf.clear();
      charBuffer = CharBuffer.wrap("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" +
          "AFyMJ4kLsONE52kZsJYYyDpWw9a8BZ");

      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());

      // wide character string.
      sqlPatternComplex = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf));

      // non ascii
      drillBuf.clear();
      charBuffer = CharBuffer.wrap("¤EÀsÆW°ê»Ú®i¶T¤¤¤ß3¼Ó®i¶TÆU2~~");
      byteBuffer = charsetEncoder.encode(charBuffer);
      drillBuf.setBytes(0, byteBuffer, byteBuffer.position(), byteBuffer.remaining());
      assertEquals(0, sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf)); // DEFGHIJ should not match A.*BC.*

      patternInfo = new RegexpUtil.SqlPatternInfo(RegexpUtil.SqlPatternType.COMPLEX, ".*»Ú®i¶T¤¤¤.*¼Ó®i.*ÆU2~~", "");
      sqlPatternComplex = SqlPatternFactory.getSqlPatternMatcher(patternInfo);
      assertEquals(1, sqlPatternComplex.match(0, byteBuffer.limit(), drillBuf)); // should match

      drillBuf.close();
      allocator.close();

    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error while encoding the pattern string ", e);
    }
  }

}
