/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalog;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.PrestoException;

import static java.lang.String.format;


public class DynamicCatalogException extends PrestoException {

    private static final long serialVersionUID = -6662948452665096247L;

    public static DynamicCatalogException newInstance(String message, Object... params) {
        return new DynamicCatalogException(format(message, params));
    }

    public static DynamicCatalogException newInstance(String message, Throwable e) {
        return new DynamicCatalogException(message, e);
    }

    private DynamicCatalogException(String message) {
        super(() -> new ErrorCode(20000, "DynamicCatalog", ErrorType.INTERNAL_ERROR), message);
    }

    private DynamicCatalogException(String message, Throwable e) {
        super(() -> new ErrorCode(20000, "DynamicCatalog", ErrorType.INTERNAL_ERROR), message, e);
    }
}
