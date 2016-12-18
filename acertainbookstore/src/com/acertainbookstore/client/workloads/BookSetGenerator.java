package com.acertainbookstore.client.workloads;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;

/**
 * Helper class to generate stockbooks and isbns modelled similar to Random
 * class
 */
public class BookSetGenerator {
	
	private static int latestIsbn = 0;
	
	public BookSetGenerator() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Returns num randomly selected isbns from the input set
	 * 
	 * @param num
	 * @return
	 */
	public Set<Integer> sampleFromSetOfISBNs(Set<Integer> isbns, int num) throws BookStoreException {
		
		// If the given number num is larger than the number of isbns, throw exception
		if (num > isbns.size()) {
			throw new BookStoreException(BookStoreConstants.BOOK_NUM_PARAM + BookStoreConstants.INVALID);
		}
		
		// The list of selected isbns
		Set<Integer> selectedIsbns = new HashSet<Integer>();
		
		// Create random generator
		Random rn = new Random();		
		int randIndex = -1;
		for (int i = 0; i < num; i++) {
			// Get next random index
			randIndex = rn.nextInt(isbns.size());	
			// Get the isbn
			Integer curValue = (Integer) isbns.toArray()[randIndex];
			// Add it to the Set we want to return
			selectedIsbns.add(curValue);
			// Remove the selected isbn from the original set, such that we don't select again
			isbns.remove(curValue);
		}		
		
		return selectedIsbns;		
	}

	/**
	 * Return num stock books. For now return an ImmutableStockBook
	 * 
	 * @param num
	 * @return
	 */
	public Set<StockBook> nextSetOfStockBooks(int num) {
	
		// The list of random generated stockbooks
		Set<StockBook> randStockbooks = new HashSet<StockBook>();
		
		// Create random generator
		Random rn = new Random();
		
		int bound = 10;
		
		// Now create the specified number of books
		for (int i = 0; i < num; i++) {		
			// Get current unique isbn
			int curIsbn = rn.nextInt(99999999);
			// Generate random properties
			String title = String.valueOf(rn.nextInt(bound));
			String author = String.valueOf(rn.nextInt(bound));
			float price = rn.nextFloat() + 1;
			int numCopies = rn.nextInt(bound) + 1;
			long numSaleMisses = rn.nextLong();
			long numTimesRated = rn.nextLong();
			long totalRating = rn.nextLong();
			boolean editorPick = rn.nextBoolean();
			
			// Now create the book
			ImmutableStockBook curBook = new ImmutableStockBook(curIsbn, title, author, price, numCopies, numSaleMisses,
					numTimesRated, totalRating, editorPick);
			
			// Add it to the set
			randStockbooks.add(curBook);
			
			// Iterate latestIsbn
			latestIsbn++;		
		}
		
		return randStockbooks;		
		
	}

}
