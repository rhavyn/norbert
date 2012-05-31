package com.linkedin.norbert.javacompat.network;

import java.nio.charset.Charset;
import java.security.MessageDigest;
/**
 * Created by IntelliJ IDEA.
 * User: jwang
 */
public interface HashFunction<V>{
    public long hash(V key);

    public static class MD5HashFunction implements HashFunction<String>{

        private static Charset Utf8 = Charset.forName("UTF-8");

        @Override
        public long hash(String key){
          MessageDigest md;
          try{
              md = MessageDigest.getInstance("MD5");
          }
          catch(Exception e){
              throw new RuntimeException(e.getMessage(),e);
          }

          byte[] kbytes = md.digest(key.getBytes(Utf8));
          long hc = ((long)(kbytes[3]&0xFF) << 24)
            | ((long)(kbytes[2]&0xFF) << 16)
            | ((long)(kbytes[1]&0xFF) << 8)
            | (long)(kbytes[0]&0xFF);
          return Math.abs(hc);
        }
    }

    public static class NativeObjectHashFunction<V> implements HashFunction<V>{
        @Override
        public long hash(V key){
          return key.hashCode();
        }
    }

    public static class FNVStringHashingStrategy implements HashFunction<String>{
        public static final long FNV1_64_INIT = 0xcbf29ce484222325L; // 14695981039346656037L
        private static final long FNV_64_PRIME = 0x100000001b3L; // 1099511628211L
        @Override
        public long hash(String key){
            long hval = FNV1_64_INIT;
            int len = key.length();
            for (int i=0; i<len; i++) {
              hval *= FNV_64_PRIME;
              hval ^= (long)key.charAt(i);
            }
            return hval;

        }
    }
}
