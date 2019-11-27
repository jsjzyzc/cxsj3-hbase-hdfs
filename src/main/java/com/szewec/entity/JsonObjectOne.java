package com.szewec.entity;

import java.util.Map;

public class JsonObjectOne {
    private String title;
    private String article;
    private Map<String,String> authors;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getArticle() {
        return article;
    }

    public void setArticle(String article) {
        this.article = article;
    }

    public Map<String, String> getAuthors() {
        return authors;
    }

    public void setAuthors(Map<String, String> authors) {
        this.authors = authors;
    }

    public String toString(){
        return ("{title:"+title+",article:"+article+",authors:"+authors+"}");
    }
}
