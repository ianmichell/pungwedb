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
package com.pungwe.db.engine.io.util;

import java.util.regex.Pattern;

/**
 * Created by ian on 28/07/2016.
 */
public class FileUtils {

    public static Pattern uuidFilePattern(String prefix, String suffix, String separator) {
        return Pattern.compile(prefix + separator + "([a-zA-Z0-9\\-]+)" + separator + suffix);
    }
}
