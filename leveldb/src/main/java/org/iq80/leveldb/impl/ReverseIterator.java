//
// Copyright (C) 2005-2011 Cleversafe, Inc. All rights reserved.
//
// Contact Information:
// Cleversafe, Inc.
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// licensing@cleversafe.com
//
// END-OF-HEADER
//-----------------------
// @author: renar
//
// Date: Jun 30, 2014
//---------------------

package org.iq80.leveldb.impl;

import java.util.Iterator;

public interface ReverseIterator<T> extends Iterator<T>
{
   T prev();
   boolean hasPrev();
}


