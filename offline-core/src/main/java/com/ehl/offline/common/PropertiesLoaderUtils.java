package com.ehl.offline.common;/*

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/09/16
 * Time: 13:44
 */

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesLoaderUtils {
    /**
     * Load all properties from the specified class path resource
     * (in ISO-8859-1 encoding), using the default class loader.
     * <p>Merges properties if more than one resource of the same name
     * found in the class path.
     * @param resourceName the name of the class path resource
     * @return the populated Properties instance
     * @throws IOException if loading failed
     */
    public static Properties loadAllProperties(String resourceName) throws IOException {
        return loadAllProperties(resourceName, null);
    }

    /**
     * Load all properties from the specified class path resource
     * (in ISO-8859-1 encoding), using the given class loader.
     * <p>Merges properties if more than one resource of the same name
     * found in the class path.
     * @param resourceName the name of the class path resource
     * @param classLoader the ClassLoader to use for loading
     * (or {@code null} to use the default class loader)
     * @return the populated Properties instance
     * @throws IOException if loading failed
     */
    public static Properties loadAllProperties(String resourceName, ClassLoader classLoader) throws IOException {
        Preconditions.checkNotNull(resourceName, "resource null");
        ClassLoader classLoaderToUse = classLoader;
        if (classLoaderToUse == null) {
            classLoaderToUse = Thread.currentThread().getContextClassLoader();
        }
        Enumeration<URL> urls = (classLoaderToUse != null ? classLoaderToUse.getResources(resourceName) :
                ClassLoader.getSystemResources(resourceName));

        Properties props = new Properties();
        if (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            URLConnection con = url.openConnection();

            InputStream is = con.getInputStream();
            try {
                props.load(new InputStreamReader(is,"utf-8"));
            }
            finally {
                is.close();
            }
        }else{
            Path conf = Paths.get(resourceName);
            try(BufferedReader br = Files.newBufferedReader(conf,Charsets.UTF_8)){
                props.load(br);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return props;
    }

}
