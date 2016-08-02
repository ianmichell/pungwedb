/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.core.utils.comparators;

import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by ian on 28/07/2016.
 */
public class GenericComparator implements Comparator<Object> {

    private static Comparator<Object> INSTANCE;

    public static Comparator<Object> getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new GenericComparator();
        }
        return INSTANCE;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return -1;
        } else if (o2 == null) {
            return 1;
        }
        // UUID
        if (UUID.class.isAssignableFrom(o1.getClass()) && UUID.class.isAssignableFrom(o2.getClass())) {
            return ((UUID)o1).compareTo((UUID)o2);
        }
        // Compare numbers
        if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass())) {
            return new BigDecimal(o1.toString()).compareTo(new BigDecimal(o2.toString()));
        }
        // If String
        if (String.class.isAssignableFrom(o1.getClass()) && String.class.isAssignableFrom(o2.getClass())) {
            return ((String)o1).compareTo((String)o2);
        }
        // Boolean
        if (Boolean.class.isAssignableFrom(o1.getClass()) && Boolean.class.isAssignableFrom(o2.getClass())) {
            return ((Boolean)o1).compareTo((Boolean)o2);
        }
        // Date
        if (Date.class.isAssignableFrom(o1.getClass()) && Date.class.isAssignableFrom(o2.getClass())) {
            return ((Date)o1).compareTo((Date)o2);
        }
        // Calendar
        if (Calendar.class.isAssignableFrom(o1.getClass()) && Calendar.class.isAssignableFrom(o2.getClass())) {
            return ((Calendar)o1).compareTo((Calendar)o2);
        }
        // DateTime
        if (DateTime.class.isAssignableFrom(o1.getClass()) && DateTime.class.isAssignableFrom(o2.getClass())) {
            return ((DateTime)o1).compareTo((DateTime)o2);
        }
        // Map
        if (Map.class.isAssignableFrom(o1.getClass()) && Map.class.isAssignableFrom(o2.getClass())) {
            return MapComparator.getInstance().compare((Map<?, ?>)o1, (Map<?, ?>)o2);
        }
        // Everything else...
        return Integer.compare(o1.hashCode(), o2.hashCode());
    }
}
