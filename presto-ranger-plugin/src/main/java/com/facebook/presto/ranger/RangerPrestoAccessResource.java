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
package com.facebook.presto.ranger;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.HashMap;
import java.util.Map;

public class RangerPrestoAccessResource
        extends RangerAccessResourceImpl
{
    private static final String KEY_CATALOG = "catalog";
    private static final String KEY_DATABASE = "database";
    private static final String KEY_TABLE = "table";

    public static RangerPrestoAccessResource getCatalog(String catalogName)
    {
        Map<String, Object> elements = new HashMap<>();
        elements.put(KEY_CATALOG, catalogName);
        return new RangerPrestoAccessResource(elements);
    }

    public RangerPrestoAccessResource(Map<String, Object> elements)
    {
        super(elements);
    }

    public RangerPrestoAccessResource(String catalogName, String databaseName, String tableName)
    {
        setValue(KEY_CATALOG, catalogName);
        setValue(KEY_DATABASE, databaseName);
        setValue(KEY_TABLE, tableName);
    }

    public void setCatalogName(String name)
    {
        setValue(KEY_CATALOG, name);
    }

    public String getCatalogName()
    {
        return (String) getValue(KEY_CATALOG);
    }

    public void setDatabaseName(String name)
    {
        setValue(KEY_DATABASE, name);
    }

    public String getDatabaseName()
    {
        return (String) getValue(KEY_DATABASE);
    }

    public void setTableName(String name)
    {
        setValue(KEY_TABLE, name);
    }

    public String getTableName()
    {
        return (String) getValue(KEY_TABLE);
    }
}
