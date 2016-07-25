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
package com.pungwe.db.core.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * Created by ian on 24/07/2016.
 */
public abstract class TypeReference<E> {

    private Type type;

    protected TypeReference() {
        Type superClass = getClass().getGenericSuperclass();
        if (!ParameterizedType.class.isAssignableFrom(superClass.getClass())) {
            throw new IllegalArgumentException("Internal error: constructed without type information");
        }
        type = ((ParameterizedType)superClass).getActualTypeArguments()[0];
    }

    @SuppressWarnings("unchecked")
    public Class<E> getGenericType() {
        return type instanceof ParameterizedType ? (Class<E>)((ParameterizedType) type).getRawType() : (Class<E>)type;
    }
}
