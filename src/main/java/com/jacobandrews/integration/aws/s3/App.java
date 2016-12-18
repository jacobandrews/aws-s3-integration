package com.jacobandrews.integration.aws.s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Starts the Spring Context and will initialize the Spring Integration routes.
 *
 * @author Mark Borg
 * @since 1984
 *
 */
public class App 
{

	private static final Logger LOGGER = Logger.getLogger(App.class);
	private static final String HORIZONTAL_LINE = "\n=========================================================";	
	private static final String ACCESS_KEY = "AKIAJI3BYJFST6MFTTVQ";	
	private static final String BUCKET_NAME = "mark-borg";
	private static final String REMOTE_DIRECTORY = "remoteDirectory";

	/**
	 * Load the Spring Integration Application Context
	 *
	 * @param args - command line arguments
	 */
	public static void main(final String... args) throws AmazonClientException{

		final Scanner scanner = new Scanner(System.in);

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info(HORIZONTAL_LINE
					  + "\n                                                         "
					  + "\n     Welcome to the Spring Integration Amazon S3 Sample  "
					  + "\n                                                         "
					  + "\n    For more information please visit:                   "
					  + "\nhttps://github.com/SpringSource/spring-integration-extensions"
					  + "\n                                                         "
					  + HORIZONTAL_LINE );
		}

		final GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		final ConfigurableEnvironment environment = context.getEnvironment();
		
		/*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        
        AmazonS3 s3client = new AmazonS3Client(credentials);
        
        // Display options
        printMenu();
        
		String filePath;

		while (true) {
			final String input = scanner.nextLine();

			if("1".equals(input.trim())) {
				System.out.println("Uploading to Amazon S3...");
				System.out.print("\nPlease enter the path to the file you want to upload: ");
				filePath = scanner.nextLine();
		        uploadFile(environment, s3client, filePath);
				break;
			}
			else if("2".equals(input.trim())) {
				System.out.println("List files from Amazon S3...");
				listRemoteDirectory(s3client, BUCKET_NAME);
				break;				
			}
			else if("3".equals(input.trim())) {
				System.out.println("Downloading files from Amazon S3...");
				System.out.print("\nPlease enter the path to the file you want to upload: ");
				filePath = scanner.nextLine();
				downloadFile(s3client, BUCKET_NAME, filePath);
				break;
			}
			else if("q".equals(input.trim())) {
				System.out.println("Exiting application...bye.");
				System.exit(0);
			}
			else {
				System.out.println("Invalid choice\n\n");
				System.out.print("Enter you choice: ");
			}
			printMenu();
		}


		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("Exiting application...bye.");
		}

		System.exit(0);

	}
	private static void printMenu() {
		System.out.println("What would you like to do?");
		System.out.println("\t1. Upload a file to Amazon S3");
		System.out.println("\t2. List files from Amazon S3");
		System.out.println("\t3. Download files from Amazon S3");
		System.out.println("\tq. Quit the application");
		System.out.print(" > ");
	}
	
	private static void uploadFile(ConfigurableEnvironment environment, AmazonS3 s3client, String filePath) {
		try {
	        System.out.println("Uploading a new object to S3 from a file\n");
	        File file = new File(filePath);
	        
	        // Uploading file to S3 Bucket
	        s3client.putObject(new PutObjectRequest(BUCKET_NAME, file.getName(), file));
	
	     } catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which " +
	        		"means your request made it " +
	                "to Amazon S3, but was rejected with an error response" +
	                " for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    } catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which " +
	        		"means the client encountered " +
	                "an internal error while trying to " +
	                "communicate with S3, " +
	                "such as not being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	    } catch (Exception ex) {
	    	System.out.println("Error Message: " + ex.getMessage());
	    }
	}
	
	private static void listRemoteDirectory(AmazonS3 s3client, String bucket) {
        try {
            System.out.println("Listing objects");
            final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(2);
            ListObjectsV2Result result;
            do {               
               result = s3client.listObjectsV2(req);
               
               for (S3ObjectSummary objectSummary : 
                   result.getObjectSummaries()) {
                   System.out.println(" - " + objectSummary.getKey() + "  " +
                           "(size = " + objectSummary.getSize() + 
                           ")");
               }
               System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
               req.setContinuationToken(result.getNextContinuationToken());
            } while(result.isTruncated() == true ); 
            
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
            		"which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
            		"which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}
	
	private static void downloadFile(AmazonS3 s3client, String bucket, String fileName) {

	 try {
         System.out.println("Downloading an object");
         S3Object s3object = s3client.getObject(new GetObjectRequest(
        		 bucket, fileName));
         System.out.println("Content-Type: "  + 
         		s3object.getObjectMetadata().getContentType());
         displayTextInputStream(s3object.getObjectContent());
         
        // Get a range of bytes from an object.
         
         GetObjectRequest rangeObjectRequest = new GetObjectRequest(
        		 bucket, fileName);
         rangeObjectRequest.setRange(0, 10);
         S3Object objectPortion = s3client.getObject(rangeObjectRequest);
         
         System.out.println("Printing bytes retrieved.");
         displayTextInputStream(objectPortion.getObjectContent());
         
     } catch (AmazonServiceException ase) {
         System.out.println("Caught an AmazonServiceException, which" +
         		" means your request made it " +
                 "to Amazon S3, but was rejected with an error response" +
                 " for some reason.");
         System.out.println("Error Message:    " + ase.getMessage());
         System.out.println("HTTP Status Code: " + ase.getStatusCode());
         System.out.println("AWS Error Code:   " + ase.getErrorCode());
         System.out.println("Error Type:       " + ase.getErrorType());
         System.out.println("Request ID:       " + ase.getRequestId());
     } catch (AmazonClientException ace) {
         System.out.println("Caught an AmazonClientException, which means"+
         		" the client encountered " +
                 "an internal error while trying to " +
                 "communicate with S3, " +
                 "such as not being able to access the network.");
         System.out.println("Error Message: " + ace.getMessage());
     } catch (IOException ioe) {
        System.out.println("Caught an AmazonClientException, which means"+
        		" the client encountered " +
                "an internal error while trying to " +
                "communicate with S3, " +
                "such as not being able to access the network.");
        System.out.println("Error Message: " + ioe.getMessage());
    }
 }

 private static void displayTextInputStream(InputStream input)
 throws IOException {
 	// Read one text line at a time and display.
     BufferedReader reader = new BufferedReader(new 
     		InputStreamReader(input));
     while (true) {
         String line = reader.readLine();
         if (line == null) break;

         System.out.println("    " + line);
     }
     System.out.println();
 }
}
