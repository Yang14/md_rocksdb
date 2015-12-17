package md_rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        Options options = new Options().setCreateIfMissing(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, "/data/rocksdb/data");
            System.out.println("Hello World!");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
