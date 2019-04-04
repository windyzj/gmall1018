package com.atguigu.gmall1018.dw.publisher.bean;

import java.util.List;

public class Stat {
    String title ;

    List<Option> options;

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
