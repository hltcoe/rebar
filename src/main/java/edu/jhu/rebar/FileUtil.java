/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;
import org.itadaki.bzip2.BZip2InputStream;

public class FileUtil {
	public static Collection<String> getStringListFromFile (File f) {
		List<String> stringList = new ArrayList<String>();

		Scanner sc = null;
		try {
			if (FilenameUtils.getExtension(f.toString()).equals("bz2")) {
				FileInputStream in = new FileInputStream(f);
				BZip2InputStream bzIn = new BZip2InputStream(in, false);
				sc = new Scanner(bzIn);
			} else {
				sc = new Scanner(f);
			}
			while (sc.hasNextLine())
				stringList.add(sc.nextLine());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Couldn't find the file at: " + f + " !", e);
		} finally {
			if (sc != null)
				sc.close();
		}
		return stringList;
	}
	
	public static InputStream getInputStream (File f) throws FileNotFoundException, IOException {
		InputStream stream;
		String fileName = f.getName();
		if (fileName.matches(".*\\.(gz|GZ)$")) {
		    stream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)));
		} else if (fileName.matches(".*\\.(bz2|BZ2)$")) {
		    stream = new BZip2InputStream(new BufferedInputStream(new FileInputStream(f)), false);
		} else {
		    stream = new BufferedInputStream(new FileInputStream(f));
		}
		
		return stream;
	}
	
	public static BufferedReader getReaderFromFile(File f) throws UnsupportedEncodingException, 
			FileNotFoundException, IOException {
		return FileUtil.getReaderFromInputStream(FileUtil.getInputStream(f));
	}
	
	public static BufferedReader getReaderFromInputStream (InputStream is, String encoding) throws UnsupportedEncodingException {
		return new BufferedReader(new InputStreamReader(is, encoding));
	}
	
	public static BufferedReader getReaderFromInputStream (InputStream is) throws UnsupportedEncodingException {
		return FileUtil.getReaderFromInputStream(is, "UTF-8");
	}
}
