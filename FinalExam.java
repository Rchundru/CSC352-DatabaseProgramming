package CSC352HW;

import java.sql.*;
import java.io.*;
import java.util.*;

public class FinalExam {
	public static void main (String[] args) throws FileNotFoundException {
		
		Connection conn = null;
	    Statement stmt = null;
	    String[] ageCodes = new String[] {"1","18","25","35","45","50","56"};
	    String[] ageRange = new String[] {"Under 18","18-24","25-34","35-44","45-49","50-55","56+"};
	    String[] occupationCode = new String[] {"0","1","2","3","4","5","6","7","8","9","10","11",
	    		"12","13","14","15","16","17","18","19","20"};
	    String[] occupations = new String[] {"other","academic/educator","artist","clerical/admin",
	    		"college/grad student","customer service", "doctor/health care", "executive/managerial",
	    		"farmer", "homemaker", "K-12 student", "lawyer", "programmer", "retired","sales/marketing",
	    		"scientist","self-employed","technician/engineer","tradesman/craftsman","unemployed",
	    		"writer"};

		
		System.out.print( "\nLoading JDBC driver...\n\n" );
	      try {
	         Class.forName("oracle.jdbc.OracleDriver");
	         }
	      catch(ClassNotFoundException e) {
	         System.out.println(e);
	         System.exit(1);
	         }
	      
	      try {
	          System.out.print( "Connecting to DEF database...\n\n" );
	          conn = DriverManager.getConnection("jdbc:oracle:thin:@140.192.30.237:1521:def", "rchundru", "cdm1888308");
	          System.out.println( "Connected to database DEF...\n" );
	          stmt = conn.createStatement();
	          }
	       catch (SQLException se) {
	          System.out.println(se);
	          System.exit(1);
	          }
	      //drops the tables if there are any.
	      try {
	          String dropJUSERSString = "DROP TABLE JUSERS";
	          System.out.println("Dropping JUSERS Table if it exists...");
	          stmt.executeUpdate(dropJUSERSString);
	      }
	       catch (SQLException se) {}
	      try {
	          String dropJAgesString = "DROP TABLE JAGES";
	          System.out.println("Dropping JAGES Table if it exists...");
	          stmt.executeUpdate(dropJAgesString);
	      }
	       catch (SQLException se) {}
	      try {
	          String dropJOccupationString = "DROP TABLE JOCCUPATIONS";
	          System.out.println("Dropping JOCCUPATIONS Table if it exists...");
	          stmt.executeUpdate(dropJOccupationString);
	      }
	       catch (SQLException se) {}
	      try {
	          String dropJRATINGSString = "DROP TABLE JRATINGS";
	          System.out.println("Dropping JRATINGS Table if it exists...");
	          stmt.executeUpdate(dropJRATINGSString);
	      }
	       catch (SQLException se) {}
	      try {
	          String dropJMovieYearString = "DROP TABLE JMovieYear";
	          System.out.println("Dropping JMovieYear Table if it exists...");
	          stmt.executeUpdate(dropJMovieYearString);
	      }
	       catch (SQLException se) {}
	      try {
	          String dropJMovieCategoryString = "DROP TABLE JMovieCategory";
	          System.out.println("Dropping JMovieCategory Table if it exists...\n");
	          stmt.executeUpdate(dropJMovieCategoryString);
	          }
	       catch (SQLException se) {}
	      
	      /*Creates and populates JAGES table which hold the age range for a given agecode in the
	       * JUSERS Table*/
	      try {
	    	  System.out.println("Building new JAGES table...");
	          String createJAGESString =
	 	             "CREATE TABLE JAGES " +
	 	             "  (AgeCode Number(2) PRIMARY KEY," +
	 	             "   ageRange VARCHAR(10))";
	          stmt.executeUpdate(createJAGESString);
	         System.out.println( "Inserting rows in JAGES table..." );

	 		 PreparedStatement updateJAGES =
	 		 conn.prepareStatement( "INSERT INTO JAGES VALUES ( ?, ? )" );
			 conn.setAutoCommit(false);
			 int i = 0;
			 for(i = 0; i<ageCodes.length; i++) {
					updateJAGES.setString( 1, ageCodes[i] );
					updateJAGES.setString( 2, ageRange[i] );
					updateJAGES.executeUpdate();
			 }
			 conn.commit();
	          System.out.println("Finished loading " + i + " records into the JAGES table...\n");
	      }
	      catch (SQLException se) {}
	      
	      /*Creates and populates JOCCUPATIONS table which hold the occupation for a given OccupationCode
	       *in the JUSERS Table*/
	      try {
	    	  System.out.println("Building new JOCCUPATIONS table...");
	          String createJOCCUPATIONSString =
	 	             "CREATE TABLE JOCCUPATIONS " +
	 	             "  (OccupationCode Number(2) PRIMARY KEY," +
	 	             "   Occupation VARCHAR(50))";
	          stmt.executeUpdate(createJOCCUPATIONSString);
	         System.out.println( "Inserting rows in JOCCUPATIONS table..." );

	 		 PreparedStatement updateJOCCUPATIONS =
	 		 conn.prepareStatement( "INSERT INTO JOCCUPATIONS VALUES ( ?, ? )" );
			 conn.setAutoCommit(false);
			 int i = 0;
			 for(i = 0; i<occupationCode.length; i++) {
					updateJOCCUPATIONS.setString( 1, occupationCode[i] );
					updateJOCCUPATIONS.setString( 2, occupations[i] );
					updateJOCCUPATIONS.executeUpdate();
			 }
			 conn.commit();
	         System.out.println("Finished loading " + i + " records into the JOCCUPATIONS table...\n");
	      }
	      catch (SQLException se) {}
	      
	      
		    //Creates and Populates the Users Table      
	      try {

	          System.out.println( "Building new JUSERS table..." );
	          String createJUSERSString =
	             "CREATE TABLE JUSERS " +
	             "  (UserID Number(10) PRIMARY KEY," +
	             "   gender VARCHAR(1), AgeCode NUMBER(2), Occupation NUMBER(2), ZipCode VARCHAR(30)," +
	             "FOREIGN KEY (AgeCode) REFERENCES JAGES(AgeCode)," +
	             "FOREIGN KEY (Occupation) REFERENCES JOCCUPATIONS(OccupationCode))";
	          stmt.executeUpdate(createJUSERSString);

	          System.out.println( "Inserting rows in JUSERS table..." );

	          PreparedStatement updateUsers =
	        			 conn.prepareStatement( "INSERT INTO JUSERS VALUES ( ?, ?, ?, ?, ? )" );
	          
	          conn.setAutoCommit(false);
	          Scanner usersFile = new Scanner (new FileReader("C:\\Users\\Rohit\\Desktop\\ml-1m\\users.dat"));
	          int usersCount = 0;
	          while(usersFile.hasNext()) {
	        	  String line = usersFile.next();
	        	  String userID = line.substring(0, line.indexOf("::"));
	        	  line = line.substring(line.indexOf("::")+2);
	        	  String gender = line.substring(0, line.indexOf("::"));
	        	  line = line.substring(line.indexOf("::")+2);
	        	  String ageCode = line.substring(0, line.indexOf("::"));
	        	  line = line.substring(line.indexOf("::")+2);
	        	  String occupation = line.substring(0, line.indexOf("::"));
	        	  line = line.substring(line.indexOf("::")+2);
	        	  String ZIPCode = line;
	        	  updateUsers.setString( 1, userID );
	        	  updateUsers.setString( 2, gender );
	        	  updateUsers.setString( 3, ageCode );
	        	  updateUsers.setString( 4, occupation );
	        	  updateUsers.setString( 5, ZIPCode );
	        	  usersCount++;
	        	  updateUsers.executeUpdate();
	          }
	          conn.commit();
	          usersFile.close(); 
	          System.out.println("Finished loading " + usersCount + " records into the JUsers table...\n");
	          
	          }
	       catch (SQLException se) {
	          System.out.println( "SQL ERROR: " + se );
	          } 
	      
	      
	      /* Creates a table to hold the movie titles along with the year. Creates another table to handle and hold
	       * the repeating categories group */
	      
	      try {

	          System.out.println( "Building new JMovieYear table..." );
	          String createJMovieYearString =
	             "CREATE TABLE JMovieYear " +
	             "  (MovieID Number(10) PRIMARY KEY," +
	             "   MovieTitle VARCHAR(100), Year VARCHAR(4))";
	          stmt.executeUpdate(createJMovieYearString);
	          
	          System.out.println( "Building new JMovieCategory table..." );
	          String createJMovieCategoryString =
	             "CREATE TABLE JMovieCategory " +
	             "  (MovieID Number(10), Categories VARCHAR(100)," +
	            		 "CONSTRAINT PK_JMovieCategory PRIMARY KEY (MovieID, Categories))";
	          stmt.executeUpdate(createJMovieCategoryString);
	          
	          System.out.println( "Inserting rows in JMovieYear & JMovieCategory tables..." );
	          
	          PreparedStatement updateMovieYear =
	        			 conn.prepareStatement( "INSERT INTO JMovieYear VALUES ( ?, ?, ? )" );
	          
	          PreparedStatement updateMovieCategory =
	        			 conn.prepareStatement( "INSERT INTO JMovieCategory VALUES ( ?, ? )" );
	          
	          int yearCount = 0;
	          int categoryCount = 0;
	          
	          conn.setAutoCommit(false);
	          Scanner MovieFile = new Scanner (new FileReader("C:\\Users\\Rohit\\Desktop\\ml-1m\\movies.dat"));
	          while(MovieFile.hasNextLine()) {
	        	  String line = MovieFile.nextLine();
	        	  String MovieID = line.substring(0, line.indexOf("::"));
	        	  line = line.substring(line.indexOf("::")+2);
	        	  int PPosition = line.indexOf("(1");
	        	  if(PPosition == -1) {
	        		  PPosition = line.indexOf("(2");
	        	  }
	        	  String MovieYear = line.substring(0, line.indexOf("::"));
	        	  String year = line.substring(PPosition+1, PPosition+5);
	        	  String MovieTitle = line.substring(0, PPosition);
	        	  line = line.substring(line.indexOf("::")+2);
	        	  updateMovieYear.setString( 1, MovieID );
	        	  updateMovieYear.setString( 2, MovieTitle );
	        	  updateMovieYear.setString( 3, year );
	        	  yearCount++;
	        	  updateMovieYear.executeUpdate();
	        	  String MovieCategory = line;
	        	  int DPosition = line.indexOf("|");
	        	  if(DPosition > 0 && MovieCategory != null) {
	        		  while(DPosition > 0) {
	        			  String category = line.substring(0,DPosition);
	        			  line = line.substring(DPosition+1, line.length());
	        			  updateMovieCategory.setString( 1, MovieID );
	    	        	  updateMovieCategory.setString( 2, category );
	    	        	  updateMovieCategory.executeUpdate();
	    	        	  categoryCount++;
	        			  DPosition = line.indexOf("|");
	        		  }
	        	  }
	        	 if(MovieCategory != null) {
	        	  updateMovieCategory.setString( 1, MovieID );
   	        	  updateMovieCategory.setString( 2, line );
   	        	  categoryCount++;
   	        	  updateMovieCategory.executeUpdate();
	        	 }
	          }
	          conn.commit();
	          MovieFile.close(); 
	          System.out.println("Finished loading " + yearCount + " records into the JMovieYear table...");
	          System.out.println("Finished loading " + categoryCount + " records into the JMovieCategory table...\n");
	          
	          
	      }
	      catch (SQLException se) {
	          System.out.println( "SQL ERROR: " + se );
	      }
	      
	      //Creates and populates the JRatings table
	      
	      
	      try {
	    	  System.out.println( "Building new JRATINGS table..." );
	          String createJRATINGSString =
	             "CREATE TABLE JRATINGS " +
	             "  (UserID Number(10), MovieID NUMBER(10), Rating NUMBER(1), TimeStamp NUMBER(10), " +
	             "CONSTRAINT PK_JRATINGS PRIMARY KEY (UserID, MovieID),"+
	             "FOREIGN KEY (UserID) REFERENCES JUSERS(UserID))";
	          stmt.executeUpdate(createJRATINGSString);
	          
	          System.out.println( "Inserting rows in JRATINGS table..." );
	          
	          PreparedStatement updateRatings =
	        			 conn.prepareStatement( "INSERT INTO JRATINGS VALUES ( ?, ?, ?, ? )" );
	          
	          conn.setAutoCommit(false);
	          Scanner ratingsFile = new Scanner (new FileReader("C:\\Users\\Rohit\\Desktop\\ml-1m\\ratings-Oracle.dat"));
	          int ratingsCount = 0;
	          while(ratingsFile.hasNext()) {
	        	  String line = ratingsFile.next();
	        	  String userID = line.substring(0, line.indexOf(","));
	        	  line = line.substring(line.indexOf(",")+1);
	        	  String movieID = line.substring(0, line.indexOf(","));
	        	  line = line.substring(line.indexOf(",")+1);
	        	  String rating = line.substring(0, line.indexOf(","));
	        	  line = line.substring(line.indexOf(",")+1);
	        	  String TimeStamp = line;
	        	  updateRatings.setString( 1, userID );
	        	  updateRatings.setString( 2, movieID );
	        	  updateRatings.setString( 3, rating );
	        	  updateRatings.setString( 4, TimeStamp );
	        	  ratingsCount++;
	        	  updateRatings.executeUpdate();
	          }
	          conn.commit();
	          ratingsFile.close(); 
	          System.out.println("Finished loading " + ratingsCount + " records into the JRATINGS table...\n");
	          
	      }
	      catch (SQLException se) {
	          System.out.println( "SQL ERROR: " + se );
	      } 
	      
	      try {
	    	  System.out.println("Closing connection to DEF database...");
	    	  stmt.close();
	          conn.close();
	          System.out.println("Connection closed.");
	      }
	      catch (SQLException se) {
	          System.out.println( "SQL ERROR: " + se );
	      }
	}

}
