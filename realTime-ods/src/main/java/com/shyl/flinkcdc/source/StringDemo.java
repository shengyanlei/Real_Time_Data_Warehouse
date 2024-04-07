package com.shyl.flinkcdc.source;

import scala.xml.dtd.PublicID;

import java.security.Principal;

public class StringDemo {
    String a = "a";
    StringBuffer b = new StringBuffer("b");
    StringBuilder c = new StringBuilder("c");

    public void sout(){
        b.append("b");
        System.out.println(a+","+b+","+c);
    }

    public void box(){
        Integer i1 =100;
        Integer i2 =100;
        Integer i3 =127;
        Integer i4 =127;

        Integer one = new Integer(100);
        Integer two = new Integer(100);

        System.out.println(i1==i2);
        System.out.println(i3==i4);

        System.out.println(one==two);

        System.out.println(i1==100);
        System.out.println(i1==one);

    }

    public static void main(String[] args) {
        StringDemo demo = new StringDemo();
        demo.sout();
        demo.box();
    }

}
