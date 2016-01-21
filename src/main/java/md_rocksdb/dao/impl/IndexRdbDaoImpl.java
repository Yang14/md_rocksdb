package md_rocksdb.dao.impl;

import com.alibaba.fastjson.JSON;
import md.mgmt.dao.IndexRdbDao;
import md.mgmt.dao.entity.DirMdIndex;
import md.mgmt.dao.entity.DistrCodeList;
import md.mgmt.dao.entity.FileMdIndex;
import md.mgmt.dao.entity.MdIndexKey;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by Mr-yang on 16-1-11.
 */
@Component
public class IndexRdbDaoImpl implements IndexRdbDao {
    private Logger logger = LoggerFactory.getLogger(IndexRdbDaoImpl.class);

    static {
        RocksDB.loadLibrary();
    }

    private static final String DB_PATH = "/data/rdb/mdIndex";
    private static final String RDB_DECODE = "UTF8";

    @Override
    public DirMdIndex getParentDirMdIndexByPath(String path) {
        if (path == null || path.equals("") || path.charAt(0) != '/') {
            logger.error("findParentDirCodeByPath params err: " + path);
            return null;
        }
        if (path.equals("/")) {
            String key = JSON.toJSONString(new MdIndexKey("0", "/"));
            return getDirMdIndex(key);
        }
        String[] nodes = path.split("/");
        nodes[0] = "/";
        String code = "0";
        for (int i = 0; i < nodes.length - 1 && code != null; ++i) {
            String key = JSON.toJSONString(new MdIndexKey("0", "/"));
            code = getFileMdIndex(key).getFileCode();
        }
        return getDirMdIndex(JSON.toJSONString(new MdIndexKey(code, nodes[nodes.length - 1])));
    }


    @Override
    public boolean putFileMdIndex(String key, FileMdIndex fileMdIndex) {
        Options options = new Options().setCreateIfMissing(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, DB_PATH);
            db.put(key.getBytes(RDB_DECODE),JSON.toJSONString(fileMdIndex).getBytes());
            return true;
        } catch (Exception e) {
            logger.error(String.format("[ERROR] caught the unexpceted exception -- %s\n", e));
        } finally {
            if (db != null) db.close();
            options.dispose();
        }
        return false;
    }

    private FileMdIndex getFileMdIndex(String key) {
        Options options = new Options().setCreateIfMissing(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, DB_PATH);
            byte[] indexBytes = db.get(key.getBytes(RDB_DECODE));
            if (indexBytes != null) {
                String indexValue = new String(indexBytes, RDB_DECODE);
                return JSON.parseObject(indexValue, FileMdIndex.class);
            }
        } catch (Exception e) {
            logger.error(String.format("[ERROR] caught the unexpceted exception -- %s\n", e));
        } finally {
            if (db != null) db.close();
            options.dispose();
        }
        return null;
    }

    @Override
    public DirMdIndex getDirMdIndex(String key) {
        Options options = new Options().setCreateIfMissing(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, DB_PATH);
            byte[] indexBytes = db.get(key.getBytes(RDB_DECODE));
            if (indexBytes != null) {
                String indexValue = new String(indexBytes, RDB_DECODE);
                FileMdIndex fileMdIndex = JSON.parseObject(indexValue, FileMdIndex.class);
                String fileCode = fileMdIndex.getFileCode();
                byte[] distrCodeBytes = db.get(fileCode.getBytes(RDB_DECODE));
                if (distrCodeBytes != null) {
                    String distrCodeValue = new String(distrCodeBytes, RDB_DECODE);
                    DistrCodeList distrCodeList = JSON.parseObject(distrCodeValue, DistrCodeList.class);
                    return new DirMdIndex(fileMdIndex, distrCodeList);
                }
            }
        } catch (Exception e) {
            logger.error(String.format("[ERROR] caught the unexpceted exception -- %s\n", e));
        } finally {
            if (db != null) db.close();
            options.dispose();
        }
        return null;
    }

    @Override
    public boolean putDistrCodeList(String key, DistrCodeList distrCodeList) {
        Options options = new Options().setCreateIfMissing(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, DB_PATH);
            db.put(key.getBytes(RDB_DECODE),JSON.toJSONString(distrCodeList).getBytes());
            return true;
        } catch (Exception e) {
            logger.error(String.format("[ERROR] caught the unexpceted exception -- %s\n", e));
        } finally {
            if (db != null) db.close();
            options.dispose();
        }
        return false;
    }

    @Override
    public boolean updateDistrCodeListWithNewCode(String key, String newCode) {
        return false;
    }

}
