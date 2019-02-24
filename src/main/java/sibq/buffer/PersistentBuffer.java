/*
 *
 *   Copyright (C) 219  InterviewParrot SIBQ
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package sibq.buffer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * A rocksDB backed buffer for messages.
 * @Author interviewparrot
 */
@Log4j2
@RequiredArgsConstructor
public class PersistentBuffer {

    @NonNull
    private String bufferName;

    private RocksDB rocksDB;



    public void init() throws Exception {

        Path db_path = Paths.get(bufferName);
        log.info("Cache PATH: {}" , db_path.toString());

        if (!Files.exists(db_path)) {
            if(!Files.exists(db_path.getParent())) {
                Files.createDirectory(db_path.getParent());
            }
            Files.createDirectory(db_path);
        }
        try (final Options options = new Options();
             final Filter bloomFilter = new BloomFilter(10);
             final ReadOptions readOptions = new ReadOptions()
                     .setFillCache(false);
             final Statistics stats = new Statistics();
             final RateLimiter rateLimiter = new RateLimiter(10000000, 10000, 10)) {

            options.setCreateIfMissing(true)
                    .setStatistics(stats)
                    .setWriteBufferSize(8 * SizeUnit.KB)
                    .setMaxWriteBufferNumber(3)
                    .setMaxBackgroundCompactions(10)
                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                    .setCompactionStyle(CompactionStyle.UNIVERSAL);

            options.setMemTableConfig(
                    new HashSkipListMemTableConfig()
                            .setHeight(4)
                            .setBranchingFactor(4)
                            .setBucketCount(2000000));
            options.setMemTableConfig(
                    new VectorMemTableConfig().setReservedSize(10000));
            options.setMemTableConfig(new SkipListMemTableConfig());
            options.setTableFormatConfig(new PlainTableConfig());
            // Plain-Table requires mmap read
            options.setAllowMmapReads(true);
            options.setRateLimiter(rateLimiter);

            final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
            table_options.setBlockCacheSize(64 * SizeUnit.KB)
                    .setFilter(bloomFilter)
                    .setCacheNumShardBits(6)
                    .setBlockSizeDeviation(5)
                    .setBlockRestartInterval(10)
                    .setCacheIndexAndFilterBlocks(true)
                    .setHashIndexAllowCollision(false)
                    .setBlockCacheCompressedSize(64 * SizeUnit.KB)
                    .setBlockCacheCompressedNumShardBits(10);

            options.setTableFormatConfig(table_options);
            this.rocksDB =  RocksDB.open(options, db_path.toString());
        }
    }


    public boolean put(byte[] key, byte[] data) {
        try {
            rocksDB.put(key, data);
        } catch (RocksDBException e) {
            log.warn("Put failed: ", e);
            return false;
        }

        return true;
    }

    public List<byte[]> list(byte[] prefix, int pageSize) {
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = rocksDB.newIterator();
        iterator.seek(prefix);
        int count = 0;
        List<byte[]> values = new ArrayList<>();
        while(count < pageSize) {
            values.add(iterator.value());
            iterator.next();
        }
        return values;
    }

    public boolean delete(byte[] key) {
        try {
            rocksDB.delete(key);
        } catch (RocksDBException e) {
            log.error(e);
            return false;
        }
        return true;
    }

    public byte[] get(byte[] key) {
        try {
            return rocksDB.get(key);
        } catch (RocksDBException e) {
            log.warn("Get failed: ", e);
        }
        return null;
    }

}
