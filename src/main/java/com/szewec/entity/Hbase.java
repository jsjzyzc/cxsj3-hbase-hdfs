package com.szewec.entity;

public class Hbase {
    private String tableName;
    private String[] family;
    private String familyName;
    private String rowNey;
    private String column;
    private String startRow;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    private String stopRow;
    private String url;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getFamily() {
        return family;
    }

    public void setFamily(String[] family) {
        this.family = family;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public String getRowNey() {
        return rowNey;
    }

    public void setRowNey(String rowNey) {
        this.rowNey = rowNey;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }

    public String getStopRow() {
        return stopRow;
    }

    public void setStopRow(String stopRow) {
        this.stopRow = stopRow;
    }
}
