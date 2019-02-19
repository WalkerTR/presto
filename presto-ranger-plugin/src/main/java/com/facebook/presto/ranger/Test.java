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

import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.util.HashSet;

public class Test {
    public static void main(String[] args) {
        RangerPrestoPlugin rangerPlugin = new RangerPrestoPlugin("myservice");
        rangerPlugin.init();


        RangerPrestoAccessResource accessResource =
                new RangerPrestoAccessResource("mycatalogue", "mydatabase", "mytable");

        RangerPrestoAccessRequest accessRequest = new RangerPrestoAccessRequest(accessResource,
                "testuser",
                new HashSet<>(),
                RangerPrestoAccessType.SELECT);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(accessRequest);

        System.out.println(result.getIsAllowed());
    }
}
