// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "options.h"
#include "comparator.h"
#include "env.h"

namespace prism
{

	// Options constructor: Initialize with sensible defaults
	Options::Options()
	    : comparator(BytewiseComparator()) // Default: lexicographic byte comparison
	    , env(Env::Default()) // Default: platform's default environment
	{
		// All other fields have default initializers in the header
	}

} // namespace prism
