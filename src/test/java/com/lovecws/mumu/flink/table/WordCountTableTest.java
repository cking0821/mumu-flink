package com.lovecws.mumu.flink.table;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: table word count
 * @date 2018-02-28 14:44
 */
public class WordCountTableTest {

    private WordCountTable wordCountTable = new WordCountTable();

    @Test
    public void wordCount() throws Exception {
        wordCountTable.wordCount();
    }

    @Test
    public void sqlQuery() throws Exception {
        wordCountTable.sqlQuery();
    }
}
