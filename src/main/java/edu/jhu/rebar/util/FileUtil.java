/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FilenameUtils;

public class FileUtil {
    public static Collection<String> getStringListFromFile(File f) {
        List<String> stringList = new ArrayList<String>();

        Scanner sc = null;
        try {
            if (FilenameUtils.getExtension(f.toString()).equals("bz2")) {
                FileInputStream in = new FileInputStream(f);
                BufferedInputStream bis = new BufferedInputStream(in);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
                
                sc = new Scanner(bzIn);
                bzIn.close();
            } else {
                sc = new Scanner(f);
            }
            while (sc.hasNextLine())
                stringList.add(sc.nextLine());
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (sc != null)
                sc.close();
        }
        return stringList;
    }

    public static InputStream getInputStream(File f) throws FileNotFoundException, IOException {
        InputStream stream;
        String fileName = f.getName();
        if (fileName.matches(".*\\.(gz|GZ)$")) {
            stream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)));
        } else if (fileName.matches(".*\\.(bz2|BZ2)$")) {
            stream = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(f)), false);
        } else {
            stream = new BufferedInputStream(new FileInputStream(f));
        }

        return stream;
    }
    
    /**
     * Delete folder and all subcontents. Avoids recursion so won't crash on deeply
     * nested folders. 
     * 
     * @param pathToFile - {@link Path} to the file to be deleted
     */
    public static void deleteFolderAndSubfolders(Path pathToFile) {
        File f = pathToFile.toFile();
        File[] currentFileList;
        Stack<File> stack = new Stack<>();
        stack.push(f);
        while (! stack.isEmpty()) {
            if (stack.lastElement().isDirectory()) {
                currentFileList = stack.lastElement().listFiles();
                if (currentFileList.length > 0) {
                    for (File curr: currentFileList) 
                        stack.push(curr);
                    
                } else 
                    stack.pop().delete();
                
            } else 
                stack.pop().delete();
            
        }
    }

    public static BufferedReader getReaderFromFile(File f) throws UnsupportedEncodingException, FileNotFoundException, IOException {
        return FileUtil.getReaderFromInputStream(FileUtil.getInputStream(f));
    }

    public static BufferedReader getReaderFromInputStream(InputStream is, String encoding) throws UnsupportedEncodingException {
        return new BufferedReader(new InputStreamReader(is, encoding));
    }

    public static BufferedReader getReaderFromInputStream(InputStream is) throws UnsupportedEncodingException {
        return FileUtil.getReaderFromInputStream(is, "UTF-8");
    }
}
