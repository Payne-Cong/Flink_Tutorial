package com.payne.bean;

public class Student {
    public String id;
    public String name;
    public Integer source;
    public String classes;

    public Student() {
    }

    public Student(String id, String name, Integer source, String classes) {
        this.id = id;
        this.name = name;
        this.source = source;
        this.classes = classes;
    }

    public Student(String name, Integer source) {
        this.name = name;
        this.source = source;
    }

    public Student(String name, Integer source, String classes) {
        this.name = name;
        this.source = source;
        this.classes = classes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSource() {
        return source;
    }

    public void setSource(Integer source) {
        this.source = source;
    }

    public String getClasses() {
        return classes;
    }

    public void setClasses(String classes) {
        this.classes = classes;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", source=" + source +
                ", classes='" + classes + '\'' +
                '}';
    }
}
