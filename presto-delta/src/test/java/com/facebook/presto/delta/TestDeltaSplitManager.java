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
package com.facebook.presto.delta;

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.delta.DeltaSplitManager.checkNoDeletionVector;
import static com.facebook.presto.delta.DeltaSplitManager.getNullPartitionKeys;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestDeltaSplitManager
{
    @Test
    public void testRejectsActiveDeletionVector()
    {
        DeletionVectorDescriptor deletionVector = new DeletionVectorDescriptor(
                DeletionVectorDescriptor.PATH_DV_MARKER,
                "dv.bin",
                Optional.of(0),
                1,
                1);

        PrestoException exception = expectThrows(PrestoException.class, () -> checkNoDeletionVector(deletionVector));
        assertEquals(exception.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertEquals(exception.getMessage(), "Delta Lake deletion vectors are not supported");
    }

    @Test
    public void testAcceptsFileWithoutDeletionVector()
    {
        checkNoDeletionVector(null);
    }

    @Test
    public void testIncludesMissingCurrentPartitionColumnsAsNull()
    {
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("part1", null);
        partitionValues.put("part5", "1");

        assertEquals(
                getNullPartitionKeys(
                        partitionValues,
                        ImmutableList.of(
                                new DeltaColumn(null, null, "id", BIGINT.getTypeSignature(), true, false),
                                new DeltaColumn(null, null, "part1", BIGINT.getTypeSignature(), true, true),
                                new DeltaColumn(null, null, "part2", BIGINT.getTypeSignature(), true, true))),
                ImmutableSet.of("part1", "part2"));
    }
}
