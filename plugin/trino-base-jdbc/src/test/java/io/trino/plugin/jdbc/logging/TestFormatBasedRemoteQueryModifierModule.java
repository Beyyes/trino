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
package io.trino.plugin.jdbc.logging;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.Bootstrap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFormatBasedRemoteQueryModifierModule
{
    @Test
    public void testRemoteQueryModifierAvailableByDefault()
    {
        RemoteQueryModifier remoteQueryModifier = new Bootstrap(new RemoteQueryModifierModule())
                .initialize()
                .getInstance(RemoteQueryModifier.class);

        assertThat(remoteQueryModifier)
                .isEqualTo(RemoteQueryModifier.NONE);
    }

    @Test
    public void testNonEmptyFormatProducingNonDefaultRemoteQueryModifier()
    {
        RemoteQueryModifier remoteQueryModifier = new Bootstrap(new RemoteQueryModifierModule())
                .setRequiredConfigurationProperties(ImmutableMap.of("query.comment-format", "valid format"))
                .initialize()
                .getInstance(RemoteQueryModifier.class);

        assertThat(remoteQueryModifier)
                .isNotEqualTo(RemoteQueryModifier.NONE);
    }
}
