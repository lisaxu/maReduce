package pa1;
import java.nio.charset.StandardCharsets;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class ParserTest {
	static String readFile(String path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
			}
	
	public String getAuthor(String wholeFile) {
		int start = wholeFile.indexOf("Author: ");
		int end = wholeFile.indexOf('\n', start);
		start += wholeFile.substring(start, end).lastIndexOf(' ') + 1;
		System.out.println(start);
		System.out.println(end);
		
		return wholeFile.substring(start, end);
	}
	
    public int getYear(String wholeFile) {
    	int start = wholeFile.indexOf("Release");
    	start = wholeFile.indexOf(',', start);
		return Integer.parseInt(wholeFile.substring(start+ 2, start + 6));
	}
	
    public int getBookID(String wholeFile) {
    	int start = wholeFile.indexOf("[EBook");
    	int end = wholeFile.indexOf(']', start);
    	return (Integer.parseInt(wholeFile.substring(start + 8,end)));
    
	}
    
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String test = "The Project Gutenberg EBook of A Discourse on Method, by René Descartes\nThis eBook is for the use of anyone anywhere at no cost and with\nalmost no restrictions whatsoever.  You may copy it, give it away or\nre-use it under the terms of the Project Gutenberg License included\nwith this eBook or online at www.gutenberg.net\n\n\nTitle: A Discourse on Method\n\nAuthor: René Descartes\n\nRelease Date: July 1, 2008 [EBook #59]\n\nLanguage: English\n\nCharacter set encoding: ASCII\n\n*** START OF THIS PROJECT GUTENBERG EBOOK A DISCOURSE ON METHOD ***";
		
		
		ParserTest t = new ParserTest();
	
		test = t.readFile("59.txt", StandardCharsets.US_ASCII);
		System.out.println(t.getBookID(test));
	}

}
