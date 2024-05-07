package com.shyl.bean;

import javax.xml.bind.SchemaOutputResolver;

public class Student {
    private String name;
    private int age;

    public Student() {
    }
    private Student(String name, int age) {
        this.name = name;
        this.age = age;
        System.out.println("姓名：" + name + " 年龄：" + age);
    }
    protected void learn(){
        System.out.println("学习");
    }

    private void eat(){
        System.out.println("炫");
    }
}
