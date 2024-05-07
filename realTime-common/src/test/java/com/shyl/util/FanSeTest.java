package com.shyl.util;

import com.shyl.bean.Student;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FanSeTest  {

    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchFieldException {
        Class [] p={String.class, int.class};

        Student student = new Student();
        Class aClass = student.getClass();
        //获取私有方法
        Method method = aClass.getDeclaredMethod("eat");
        method.setAccessible(true);
        method.invoke(student);
        System.out.println(method.getName());
//        获取构造方法
        Constructor declaredConstructor = aClass.getDeclaredConstructor(p);
        declaredConstructor.setAccessible(true);
        Student student1 = (Student) declaredConstructor.newInstance("张三",18);
        //        获取私有变量
        Field name = aClass.getDeclaredField("name");
        name.setAccessible(true);
        name.set(student1,"李四");
        System.out.println(name.get(student1).toString());


    }
}
