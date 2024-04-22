package com.shyl.util;


public class LingMaTest {
//给定一个乱序数组，实现一个从小到大排序的功能
    public static void main(String[] args) {
        int[] arr = {3, 5, 1, 6, 2, 7, 4};
        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = 0; j < arr.length - 1 - i; j++) {
                if (arr[j] > arr[j + 1]) {
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + " ");
        }
    }

}
