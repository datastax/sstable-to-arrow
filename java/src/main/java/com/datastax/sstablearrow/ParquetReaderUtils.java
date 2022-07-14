package com.datastax.sstablearrow;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

public class ParquetReaderUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReaderUtils.class);

    /**
     * Read a Parquet file and call a callback on each batch of 100 rows. Closes each batch after use.
     *
     * @param path the path to the Parquet file, prefixed with "file:"
     * @param callback the callback to call on each batch of 100 rows
     *
     * @return whether or not the operation terminated successfully
     * @throws Exception if an error occurs in try-with-resources
     */
    public static <T> T read(String path, Function<VectorSchemaRoot, T> callback, ScanOptions scanOptions) throws Exception
    {
        LOGGER.debug("reading parquet file at {} with batch size {}", path, scanOptions.getBatchSize());
        try (DatasetFactory factory = new FileSystemDatasetFactory(ArrowUtils.ALLOCATOR, NativeMemoryPool.getDefault(), FileFormat.PARQUET, path);
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(scanOptions))
        {
            for (ScanTask t : scanner.scan())
            {
                try (ArrowReader reader = t.execute())
                {
                    while (reader.loadNextBatch())
                    {
                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot())
                        {
                            T error = callback.apply(root);
                            if (error != null) return error;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static <T> T read(String path, Function<VectorSchemaRoot, T> callback) throws Exception
    {
        return read(path, callback, new ScanOptions(100));
    }
}
