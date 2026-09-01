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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.log.Level;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.delta.AbstractDeltaDistributedQueryTestBase;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

final class NativeDeltaTestUtils
{
    private static Path extractedResourceDirectory;

    private NativeDeltaTestUtils() {}

    public static QueryRunner createQueryRunner(BiConsumer<QueryRunner, String> tableRegistrar)
            throws Exception
    {
        return createQueryRunner(tableRegistrar, builder -> {});
    }

    public static QueryRunner createQueryRunner(
            BiConsumer<QueryRunner, String> tableRegistrar,
            Consumer<PrestoNativeQueryRunnerUtils.DeltaQueryRunnerBuilder> queryRunnerCustomizer)
            throws Exception
    {
        Logging.initialize().setLevel("io.delta.kernel", Level.ERROR);
        PrestoNativeQueryRunnerUtils.DeltaQueryRunnerBuilder builder = PrestoNativeQueryRunnerUtils.nativeDeltaQueryRunnerBuilder()
                .addExtraProperties(ImmutableMap.of(
                        "experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"));
        queryRunnerCustomizer.accept(builder);
        QueryRunner queryRunner = builder.build();
        for (String tableName : AbstractDeltaDistributedQueryTestBase.DELTA_TEST_TABLE_LIST) {
            tableRegistrar.accept(queryRunner, tableName);
        }
        return queryRunner;
    }

    public static String localResourcePath(String resourceName)
    {
        String resourceToResolve = resourceName.isEmpty() ? "delta_v1" : resourceName;
        URL resource = requireNonNull(
                AbstractDeltaDistributedQueryTestBase.class.getClassLoader().getResource(resourceToResolve),
                "Resource not found: " + resourceToResolve);
        try {
            URI resourceUri = resource.toURI();
            if (!"jar".equals(resourceUri.getScheme())) {
                return resourceName.isEmpty() ? Path.of(resourceUri).getParent().toUri().toString() : resourceUri.toString();
            }
            Path extractedResource = extractFromJar(resourceToResolve, resourceUri);
            return resourceName.isEmpty() ? extractedResource.getParent().toUri().toString() : extractedResource.toUri().toString();
        }
        catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to resolve Delta test resource: " + resourceName, e);
        }
    }

    private static synchronized Path extractFromJar(String resourceName, URI resourceUri)
            throws IOException
    {
        if (extractedResourceDirectory == null) {
            Path dataDirectory = Path.of(requireNonNull(System.getProperty("DATA_DIR"), "DATA_DIR is not set"));
            Files.createDirectories(dataDirectory);
            extractedResourceDirectory = Files.createTempDirectory(dataDirectory, "presto-native-delta-");
        }

        Path target = extractedResourceDirectory.resolve(resourceName);
        if (Files.exists(target)) {
            return target;
        }

        String resourceUriString = resourceUri.toString();
        URI jarUri = URI.create(resourceUriString.substring(0, resourceUriString.indexOf("!/")));
        FileSystem fileSystem;
        boolean closeFileSystem = false;
        try {
            fileSystem = FileSystems.newFileSystem(jarUri, ImmutableMap.of());
            closeFileSystem = true;
        }
        catch (FileSystemAlreadyExistsException ignored) {
            fileSystem = FileSystems.getFileSystem(jarUri);
        }

        try {
            copyRecursively(fileSystem.getPath("/" + resourceName), target);
        }
        finally {
            if (closeFileSystem) {
                fileSystem.close();
            }
        }
        return target;
    }

    private static void copyRecursively(Path source, Path target)
            throws IOException
    {
        Files.walkFileTree(source, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes)
                    throws IOException
            {
                Files.createDirectories(target.resolve(source.relativize(directory).toString()));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                    throws IOException
            {
                Files.copy(file, target.resolve(source.relativize(file).toString()));
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
