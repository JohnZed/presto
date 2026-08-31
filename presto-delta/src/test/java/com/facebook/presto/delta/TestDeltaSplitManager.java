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
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.delta.DeltaSplitManager.checkNoDeletionVector;
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
}
