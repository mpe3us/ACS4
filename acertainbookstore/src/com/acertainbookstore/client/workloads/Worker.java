/**
 * 
 */
package com.acertainbookstore.client.workloads;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import com.acertainbookstore.business.Book;
import com.acertainbookstore.business.BookCopy;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.utils.BookStoreException;

/**
 * 
 * Worker represents the workload runner which runs the workloads with
 * parameters using WorkloadConfiguration and then reports the results
 * 
 */
public class Worker implements Callable<WorkerRunResult> {
    private WorkloadConfiguration configuration = null;
    private int numSuccessfulFrequentBookStoreInteraction = 0;
    private int numTotalFrequentBookStoreInteraction = 0;

    public Worker(WorkloadConfiguration config) {
	configuration = config;
    }

    /**
     * Run the appropriate interaction while trying to maintain the configured
     * distributions
     * 
     * Updates the counts of total runs and successful runs for customer
     * interaction
     * 
     * @param chooseInteraction
     * @return
     */
    private boolean runInteraction(float chooseInteraction) {
	try {
	    float percentRareStockManagerInteraction = configuration.getPercentRareStockManagerInteraction();
	    float percentFrequentStockManagerInteraction = configuration.getPercentFrequentStockManagerInteraction();

	    if (chooseInteraction < percentRareStockManagerInteraction) {
		runRareStockManagerInteraction();
	    } else if (chooseInteraction < percentRareStockManagerInteraction
		    + percentFrequentStockManagerInteraction) {
		runFrequentStockManagerInteraction();
	    } else {
		numTotalFrequentBookStoreInteraction++;
		runFrequentBookStoreInteraction();
		numSuccessfulFrequentBookStoreInteraction++;
	    }
	} catch (BookStoreException ex) {
	    return false;
	}
	return true;
    }

    /**
     * Run the workloads trying to respect the distributions of the interactions
     * and return result in the end
     */
    public WorkerRunResult call() throws Exception {
	int count = 1;
	long startTimeInNanoSecs = 0;
	long endTimeInNanoSecs = 0;
	int successfulInteractions = 0;
	long timeForRunsInNanoSecs = 0;

	Random rand = new Random();
	float chooseInteraction;

	// Perform the warmup runs
	while (count++ <= configuration.getWarmUpRuns()) {
	    chooseInteraction = rand.nextFloat() * 100f;
	    runInteraction(chooseInteraction);
	}

	count = 1;
	numTotalFrequentBookStoreInteraction = 0;
	numSuccessfulFrequentBookStoreInteraction = 0;

	// Perform the actual runs
	startTimeInNanoSecs = System.nanoTime();
	while (count++ <= configuration.getNumActualRuns()) {
	    chooseInteraction = rand.nextFloat() * 100f;
	    if (runInteraction(chooseInteraction)) {
		successfulInteractions++;
	    }
	}
	endTimeInNanoSecs = System.nanoTime();
	timeForRunsInNanoSecs += (endTimeInNanoSecs - startTimeInNanoSecs);
	return new WorkerRunResult(successfulInteractions, timeForRunsInNanoSecs, configuration.getNumActualRuns(),
		numSuccessfulFrequentBookStoreInteraction, numTotalFrequentBookStoreInteraction);
    }

    /**
     * Runs the new stock acquisition interaction
     * 
     * @throws BookStoreException
     */
    private void runRareStockManagerInteraction() throws BookStoreException {
    	
    	List<StockBook> booksFromStore = this.configuration.getStockManager().getBooks();
    	Set<StockBook> randomBooks = this.configuration.getBookSetGenerator().nextSetOfStockBooks(this.configuration.getNumBooksToAdd());
    	
    	// Set of books we want to add
		Set<StockBook> booksToAdd = new HashSet<StockBook>();
		
		boolean bookIsAlreadyInStore = false;
		
		// Add all books to booksToAdd with an isnb not already present in the StockManager
		for (StockBook book : randomBooks) {
			
			for (StockBook bookInStore : booksFromStore) {
				if (book.getISBN() == bookInStore.getISBN()) {
					bookIsAlreadyInStore = true;
					break;
				}
			}
			
			// Now add the book if there was no isbn match
			if (!bookIsAlreadyInStore) {
				booksToAdd.add(book);
			}
			
			// Reset flag
			bookIsAlreadyInStore = false;		
			
		}			
		
		// Add them to the StockManager
		this.configuration.getStockManager().addBooks(booksToAdd);  	
    	
    }

    /**
     * Runs the stock replenishment interaction
     * 
     * @throws BookStoreException
     */
    private void runFrequentStockManagerInteraction() throws BookStoreException {
    	List<StockBook> booksFromStore = this.configuration.getStockManager().getBooks();
    	
		// Sort books according to smallest quantity of copies
		Collections.sort(booksFromStore, new Comparator<StockBook>() {
		       public int compare(StockBook o1, StockBook o2) {
		    	   int quantity1 = o1.getNumCopies();
		    	   int quantity2 = o2.getNumCopies();
		    	   
		    	   if (quantity1 > quantity2) return 1;
			       if (quantity1 < quantity2) return -1;
			       return 0;
		       }
		   });
    
		
    	// Set of books we want to add
		Set<BookCopy> copiesToAdd = new HashSet<BookCopy>();
		
		StockBook curBook;
		BookCopy curCopy;
		
		// Now pick the first numBooksWithLeastCopies books with smallest quantaties from the sorted list
		for (int i = 0; i < this.configuration.getNumBooksWithLeastCopies(); i++) {
			curBook = booksFromStore.get(i);
			curCopy = new BookCopy(curBook.getISBN(), this.configuration.getNumAddCopies());
			copiesToAdd.add(curCopy);
		}
    
		// Add them to the StockManager
		this.configuration.getStockManager().addCopies(copiesToAdd);  	
		
    }

    /**
     * Runs the customer interaction
     * 
     * @throws BookStoreException
     */
    private void runFrequentBookStoreInteraction() throws BookStoreException {
	  
    	// Get editor picks
    	List<Book> books = this.configuration.getBookStore().getEditorPicks(this.configuration.getNumEditorPicksToGet());
    	
    	// Create a set with all of the book's isbns
    	Set<Integer> bookIsbns = new HashSet<Integer>();    	
    	for (Book b : books) {
    		bookIsbns.add(b.getISBN());
    	}
    	
    	// Get a subset of the isbns
    	Set<Integer> isbnsSubset = this.configuration.getBookSetGenerator().sampleFromSetOfISBNs(bookIsbns, this.configuration.getNumBooksToBuy());
    	    	
    	// Now get books we want to buy from the selected subset
		Set<BookCopy> booksToBuy = new HashSet<BookCopy>();
    	for (Book b : books) {
    		if (isbnsSubset.contains(b.getISBN())) {
    			booksToBuy.add(new BookCopy(b.getISBN(), this.configuration.getNumBookCopiesToBuy()));
    		}
    	}
    	
    	// Now buy the books
    	this.configuration.getBookStore().buyBooks(booksToBuy);
    }

}
