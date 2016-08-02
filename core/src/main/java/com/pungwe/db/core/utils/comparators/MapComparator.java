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

import java.util.Comparator;
import java.util.Map;

/**
 * Created by ian on 30/07/2016.
 */
public class MapComparator implements Comparator<Map<?, ?>> {

    private static final MapComparator instance = new MapComparator();

    private MapComparator() {

    }

    public static MapComparator getInstance() {
        return instance;
    }

    public int compare(Map<?, ?> o1, Map<?, ?> o2) {

        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return -1;
        } else if (o2 == null) {
            return 1;
        }

        if (o1.size() > o2.size()) {
            return 1;
        } else if (o1.size() < o2.size()) {
            return -1;
        }

        if (o1.containsKey("_id") && o2.containsKey("_id")) {
            return GenericComparator.getInstance().compare(o1.get("_id"), o2.get("_id"));
        }

        int cmp = 0;
        for (Object key : o1.keySet()) {
            if (!o2.containsKey(key)) {
                return -1;
            }
            if ((cmp = GenericComparator.getInstance().compare(o1.get(key), o2.get(key))) != 0) {
                return cmp;
            }
        }
        return cmp;
    }
}
