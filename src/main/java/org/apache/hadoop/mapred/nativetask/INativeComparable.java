/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.nativetask;

/**
 * INativeComparable is a tag interface to identify a key type is comparable in native
 * and thus has no methods or fields.
 *
 * To implement a native comparator, the function should have the ComparatorPtr type
 *
 *   typedef int (*ComparatorPtr)(const char * src, uint32_t srcLength,
 *   const char * dest,  uint32_t destLength);
 *
 * The function passes in keys (in serialized format) and their lengths
 * such that we can compare them in the same logic as their Java comparator
 *
 * For example, a HiveKey {@see HiveKey#write} is serialized as
 * int field (containing the length of raw bytes) + raw bytes
 * When comparing two HiveKeys, we firstly read the length field. With that info,
 * we are able to compare the raw bytes (offsetting by 4 since length is 4-byte int)
 * invoking the BytesComparator provided by our library.
 *
 *   int HivePlatform::HiveKeyComparator(const char * src, uint32_t srcLength,
 *   const char * dest, uint32_t destLength) {
 *     uint32_t sl = bswap(*(uint32_t*)src);
 *     uint32_t dl = bswap(*(uint32_t*)dest);
 *     return NativeObjectFactory::BytesComparator(src + 4, sl, dest + 4, dl);
 *   }
 */
public interface INativeComparable {

}
