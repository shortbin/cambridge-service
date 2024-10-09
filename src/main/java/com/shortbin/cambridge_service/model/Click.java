package com.shortbin.cambridge_service.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "example", name = "wordcount")
public class Click {

    @Column(name = "word")
    private String word = "";

    @Column(name = "count")
    private long count = 0;

    public Click() {
    }

    public Click(String word, long count) {
        this.setWord(word);
        this.setCount(count);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return getWord() + " : " + getCount();
    }

}
