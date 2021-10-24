package com.daxian.api.util;

/**
 * @Author: daxian
 * @Date: 2021/10/20 7:15 下午
 */
public class main {
    public static void main(String[] args) {

        String s = "EA00002011170035";
        char c = 'E';
         System.out.println((byte)c);
       // String st = Integer.toHexString(c);
        // System.out.println((byte)c);
        //  System.out.println(st);
        parse(s);
    }

    public static String parse(String st) {
         int a=0;
        for(int i = 0; i < st.length(); i++){
            char c = st.charAt(i);
              a=a^c;
        }

        if (st.length() != 16) {
            return "非法的标签长度";
        }
        String prefix = st.substring(0, 6);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < prefix.length(); i++) {
            char c = prefix.charAt(i);
            String s = Integer.toHexString(c);
            builder.append(s);
        }

        String suffix = st.substring(10);
        String tmpString =builder.toString()+suffix;
        String st2 = Integer.toHexString(a);
        System.out.println(tmpString+st2);

        return "";
    }
}
