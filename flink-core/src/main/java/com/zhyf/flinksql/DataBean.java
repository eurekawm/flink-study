package com.zhyf.flinksql;

public class DataBean {

    public String word;
    public Integer counts;

    public DataBean() {
    }

    public DataBean(String word, Integer counts) {
        this.word = word;
        this.counts = counts;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public static DataBean of(String word, Integer count) {
        return new DataBean(word, count);
    }

    @Override
    public String toString() {
        return "DataBean{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }
}
