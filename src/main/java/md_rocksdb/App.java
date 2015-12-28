package md_rocksdb;

import org.rocksdb.*;

import java.util.Arrays;

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
        String db_path = "/data/rocksdb/testdata";
        try {
            db = RocksDB.open(options, db_path);
            System.out.println("Hello World!");
            db.put("hello".getBytes(), "world".getBytes());
            byte[] value = db.get("hello".getBytes());
            assert ("world".equals(new String(value)));
            System.out.println(new String(value));
            String str = db.getProperty("rocksdb.stats");
            assert (str != null && !str.equals(""));
            System.out.println(str);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        db.close();

        ReadOptions readOptions = new ReadOptions();
        readOptions.setFillCache(false);

        try {
            db = RocksDB.open(options, db_path);
            db.put("hello".getBytes(), "world".getBytes());
            byte[] value = db.get("hello".getBytes());
            System.out.format("Get('hello') = %s\n",
                    new String(value));

            for (int i = 1; i <= 9; ++i) {
                for (int j = 1; j <= 9; ++j) {
                    db.put(String.format("%dx%d", i, j).getBytes(),
                            String.format("%d", i * j).getBytes());
                }
            }

            for (int i = 1; i <= 9; ++i) {
                for (int j = 1; j <= 9; ++j) {
                    System.out.format("%s ", new String(db.get(
                            String.format("%dx%d", i, j).getBytes())));
                }
                System.out.println("");
            }

            // write batch test
            WriteOptions writeOpt = new WriteOptions();
            for (int i = 10; i <= 19; ++i) {
                WriteBatch batch = new WriteBatch();
                for (int j = 10; j <= 19; ++j) {
                    batch.put(String.format("%dx%d", i, j).getBytes(),
                            String.format("%d", i * j).getBytes());
                }
                db.write(writeOpt, batch);
                batch.dispose();
            }
            for (int i = 10; i <= 19; ++i) {
                for (int j = 10; j <= 19; ++j) {
                    assert (new String(
                            db.get(String.format("%dx%d", i, j).getBytes())).equals(
                            String.format("%d", i * j)));
                    System.out.format("%s ", new String(db.get(
                            String.format("%dx%d", i, j).getBytes())));
                }
                System.out.println("");
            }
            writeOpt.dispose();

            value = db.get("1x1".getBytes());
            assert (value != null);
            value = db.get("world".getBytes());
            assert (value == null);
            value = db.get(readOptions, "world".getBytes());
            assert (value == null);

            byte[] testKey = "asdf".getBytes();
            byte[] testValue =
                    "asdfghjkl;'?><MNBVCXZQWERTYUIOP{+_)(*&^%$#@".getBytes();
            db.put(testKey, testValue);
            byte[] testResult = db.get(testKey);
            assert (testResult != null);
            assert (Arrays.equals(testValue, testResult));
            assert (new String(testValue).equals(new String(testResult)));
            testResult = db.get(readOptions, testKey);
            assert (testResult != null);
            assert (Arrays.equals(testValue, testResult));
            assert (new String(testValue).equals(new String(testResult)));

            options.dispose();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        db.close();
    }
}
