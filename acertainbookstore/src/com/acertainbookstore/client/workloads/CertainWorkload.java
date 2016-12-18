/**
 * 
 */
package com.acertainbookstore.client.workloads;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.acertainbookstore.business.CertainBookStore;
import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;

/**
 * 
 * CertainWorkload class runs the workloads by different workers concurrently.
 * It configures the environment for the workers using WorkloadConfiguration
 * objects and reports the metrics
 * 
 */
public class CertainWorkload {

	
	private static final int MAX_THREADS = 10;
	private static final int THREADS_BETWEEN_ITERATIONS = 1;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		// We start with a local test
		boolean currentConfig = true;
		
		// For loop for both running local and http tests
		for (int j = 0; j < 2; j++ ) {
		
			for (int n = 1; n <= MAX_THREADS; n += THREADS_BETWEEN_ITERATIONS) {
			
				System.out.println("n:" + n);
				
				int numConcurrentWorkloadThreads = n;
				String serverAddress = "http://localhost:8081";	
				boolean localTest = currentConfig;
				List<WorkerRunResult> workerRunResults = new ArrayList<WorkerRunResult>();
				List<Future<WorkerRunResult>> runResults = new ArrayList<Future<WorkerRunResult>>();
		
				// Initialize the RPC interfaces if its not a localTest, the variable is
				// overriden if the property is set
				String localTestProperty = System
						.getProperty(BookStoreConstants.PROPERTY_KEY_LOCAL_TEST);
				localTest = (localTestProperty != null) ? Boolean
						.parseBoolean(localTestProperty) : localTest;
		
				BookStore bookStore = null;
				StockManager stockManager = null;
				if (localTest) {
					CertainBookStore store = new CertainBookStore();
					bookStore = store;
					stockManager = store;
				} else {
					stockManager = new StockManagerHTTPProxy(serverAddress + "/stock");
					bookStore = new BookStoreHTTPProxy(serverAddress);
				}
		
				// Generate data in the bookstore before running the workload
				initializeBookStoreData(bookStore, stockManager);
		
				ExecutorService exec = Executors
						.newFixedThreadPool(numConcurrentWorkloadThreads);
		
				for (int i = 0; i < numConcurrentWorkloadThreads; i++) {
					WorkloadConfiguration config = new WorkloadConfiguration(bookStore,
							stockManager);
					Worker workerTask = new Worker(config);
					// Keep the futures to wait for the result from the thread
					runResults.add(exec.submit(workerTask));
				}
		
				//System.out.println("test1");
				
				// Get the results from the threads using the futures returned
				for (Future<WorkerRunResult> futureRunResult : runResults) {
					WorkerRunResult runResult = futureRunResult.get(); // blocking call
					workerRunResults.add(runResult);
				}
		
				//System.out.println("test2");
				
				exec.shutdownNow(); // shutdown the executor
		
				//System.out.println("test3");
				
				// Finished initialization, stop the clients if not localTest
				if (!localTest) {
					((BookStoreHTTPProxy) bookStore).stop();
					((StockManagerHTTPProxy) stockManager).stop();
				}
		
				reportMetric(workerRunResults, localTest);
			}
			
			// Now run the next set of tests using HTTP
			currentConfig = false;
		}
	}

	/**
	 * Computes the metrics and prints them
	 * 
	 * @param workerRunResults
	 */
	public static void reportMetric(List<WorkerRunResult> workerRunResults, boolean localTest) {
		// TODO: You should aggregate metrics and output them for plotting here
		
		float agg_Troughput = 0f;
		float latency = 0f;		
	
		for (WorkerRunResult res : workerRunResults) {
			agg_Troughput += (float) res.getSuccessfulInteractions() / res.getElapsedTimeInNanoSecs();
			latency += (float) res.getElapsedTimeInNanoSecs() / res.getTotalRuns();
		}
		
		// Compute the average latency
		latency /= workerRunResults.size();

		//System.out.println("agg throughput: " + agg_Troughput);
		//System.out.println("latency: " + latency);
		
		
		int numConcurrentWorkloadThreads = workerRunResults.size();
		
		// Outout the metrics
		List<String> fileT_lines = Arrays.asList(agg_Troughput + ", " + numConcurrentWorkloadThreads);
	    List<String> fileL_lines = Arrays.asList(latency + ", " + numConcurrentWorkloadThreads);
	    try {
	    	if (localTest) {
	    	    Path fileT_local = Paths.get("fileT_local.txt");
	    	    Path fileL_local = Paths.get("fileL_local.txt");
	    		Files.write(fileT_local, fileT_lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
	    		Files.write(fileL_local, fileL_lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
	    	}
	    	else {
	    	    Path fileT_http  = Paths.get("fileT_http.txt");
	    	    Path fileL_http  = Paths.get("fileL_http.txt");
	    	    Files.write(fileT_http, fileT_lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
	    		Files.write(fileL_http, fileL_lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
	    	}
	    }
	    catch (Exception e) {
	    	System.out.println("Problem writing to file: " + e);
	    }
		
	}

	/**
	 * Generate the data in bookstore before the workload interactions are run
	 * 
	 * Ignores the serverAddress if its a localTest
	 * 
	 */
	public static void initializeBookStoreData(BookStore bookStore,
			StockManager stockManager) throws BookStoreException {

		// Create new BookSetGenerator
		BookSetGenerator bsg = new BookSetGenerator();
		// Get a number of randomized StockBooks
		Set<StockBook> booksToAdd = bsg.nextSetOfStockBooks(100);
		// Add them to the StockManager
		stockManager.addBooks(booksToAdd);

	}
}
