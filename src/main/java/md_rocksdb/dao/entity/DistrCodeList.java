package md_rocksdb.dao.entity;

import java.util.ArrayList;

/**
 * Created by Mr-yang on 16-1-11.
 */
public class DistrCodeList {
    private ArrayList<String> codeList;

    public ArrayList<String> getCodeList() {
        return codeList;
    }

    public void setCodeList(ArrayList<String> codeList) {
        this.codeList = codeList;
    }

    @Override
    public String toString() {
        return "DistrCodeList{" +
                "codeList=" + codeList +
                '}';
    }
}
