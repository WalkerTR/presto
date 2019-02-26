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

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RangerSystemAccessControl
        implements SystemAccessControl

{
    public static final String NAME = "ranger";

    RangerPrestoPlugin rangerPlugin;

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");
            checkArgument(config.containsKey("ranger.app-id"), "This access controller does not support any configuration properties");
            return new RangerSystemAccessControl(config.get("ranger.app-id"));
        }
    }

    public RangerSystemAccessControl(String appId)
    {
        this.rangerPlugin = new RangerPrestoPlugin(appId);
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        if (!principal.isPresent()) {
            AccessDeniedException.denySetUser(principal, userName);
        }
        AccessDeniedException.denySetUser(principal, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }

        RangerPrestoAccessResource resource = RangerPrestoAccessResource.getCatalog(catalogName);
        RangerPrestoAccessRequest request = new RangerPrestoAccessRequest(resource, identity.getUser(), new HashSet<>(), RangerPrestoAccessType.USE);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);

        if (!result.getIsAllowed()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return new HashSet<>();
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return new HashSet<>();
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return new HashSet<>();
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String grantee, boolean withGrantOption)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String revokee, boolean grantOptionFor)
    {
        if (!identity.getPrincipal().isPresent()) {
            AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
        }
        AccessDeniedException.denySetUser(identity.getPrincipal(), identity.getUser());
    }
}
