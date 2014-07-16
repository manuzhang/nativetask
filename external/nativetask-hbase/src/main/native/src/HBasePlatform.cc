/*
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

#include "string.h"

#include "NativeTask.h"
#include "lib/NativeObjectFactory.h"
#include "lib/primitives.h"
#include "HBasePlatform.h"

using NativeTask::NativeObject;
using NativeTask::NativeObjectFactory;
using NativeTask::ObjectCreatorFunc;


int HBasePlatform::ImmutableBytesWritableComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  uint32_t sl = bswap(*(uint32_t*)src);
  uint32_t dl = bswap(*(uint32_t*)dest);
  return NativeObjectFactory::BytesComparator(src + 4, sl, dest + 4, dl);
}

DEFINE_NATIVE_LIBRARY(HBasePlatform) {
  REGISTER_FUNCTION(HBasePlatform::ImmutableBytesWritableComparator, HBasePlatform);
}

