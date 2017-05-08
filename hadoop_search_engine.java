package iu.pti.hbaseapp.clueweb09;

import java.util.PriorityQueue;
import java.util.Stack;
import java.lang.*;
import java.nio.ByteBuffer;

import iu.pti.hbaseapp.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class SearchEngineTester {
	
	public static class PageRecord implements Comparable<PageRecord> {
		String docId;
		String URI;
		float pageRank;
		int termFreq;
		
		public PageRecord(String docId, String URI, float pageRank, int termFreq) {
			this.docId = docId;
			this.URI = URI;
			this.pageRank = pageRank;
			this.termFreq = termFreq;
		}
		
		@Override
		public int compareTo(PageRecord that) {
			if (pageRank < that.pageRank) {
				return -1;
			} else if (pageRank == that.pageRank) {
				return 0;
			} else {
				return 1;
			}
		}
	}
	
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.clueweb09.SearchEngineTester <command> [<parameters>]");
		System.out.println("	Where <command> <parameters> could be one of the following:");
		System.out.println("	search-keyword <keyword>");
		System.out.println("	get-page-snapshot <page document ID>");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			usage();
			System.exit(1);
		}
		
		try {
			String command = args[0];
			if (command.equals("search-keyword")) {
				searchKeyword(args[1]);
			} else if (command.equals("get-page-snapshot")) {
				getPageSnapshot(args[1]);
			} else {
				usage();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void searchKeyword(String keyword) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable dataTable = new HTable(hbaseConfig, Constants.CW09_DATA_TABLE_BYTES);
		HTable indexTable = new HTable(hbaseConfig, Constants.CW09_INDEX_TABLE_BYTES);
		HTable prTable = new HTable(hbaseConfig, Constants.CW09_PAGERANK_TABLE_BYTES);
		
		int topCount = 20;
		// this is the heap for storing the top 20 ranked pages
		PriorityQueue<PageRecord> topPages = new PriorityQueue<PageRecord>(topCount);
		
		// get the inverted index row with the given keyword
        keyword = keyword.toLowerCase();
        byte[] keywordBytes = Bytes.toBytes(keyword);
		Get gIndex = new Get(keywordBytes);
        Result indexRow = indexTable.get(gIndex);
		
        // loop through the document IDs in the row. Recall the schema of the clueWeb09IndexTable:
        // row key: term (keyword), column family: "frequencies", qualifier: document ID, cell value: term frequency in the corresponding document
		int pageCount = 0;
        for (KeyValue kv : indexRow.list()) {
            String pageDocId = null;
            int freq = 0;
            String pageUri = null;
            float pageRank = 0;

			// Write your codes for the main part of implementation here
            // Step 1: get the document ID of one page, as well as the keyword's frequency in that page
             byte[] pageDocIdbytes = kv.getQualifier();
             pageDocId = Bytes.toString(pageDocIdbytes);
             byte[] freqbytes = kv.getValue();
            // freq = ByteBuffer.wrap(freqbytes).intValue();
             freq = ByteBuffer.wrap(freqbytes).getInt();
		
            // Step 2: get the URI of the page from clueWeb09DataTable

             Get uIndex = new Get(pageDocIdbytes);
             Result uriRow = dataTable.get(uIndex);
             byte[] contents = uriRow.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES);
             pageUri = Bytes.toString(contents);
             //System.out.println("Printing URL" + pageUri);
            // Step 3: get the page rank value of this page from clueWeb09PageRankTable
             Get pgIndex = new Get(pageDocIdbytes);
             Result pgrank_row = prTable.get(pgIndex);
            // KeyValue pgkv = prTable.get(pgIndex);
            // byte[] pgrank_contents= pgrank_row.getValue(Constants.CF_PAGERANK_BYTES, null); //check this one
             //pageRank = pgrank_contents.floatValue();
         //  for (KeyValue pgkv : pgrank_row.list()){
         //    for (int i=0; i = 0; i++) {
           //  List <KeyValue> values = pgrank_row.list();
             for (KeyValue value : pgrank_row.list()) {
          // pageRank = ByteBuffer.wrap(pgrank_contents).getFloat();
          // KeyValue value = new KeyValue(pgrank_row);
           // System.out.println("I am IN??????????????"); 
           byte[] pageRank_bytes = value.getQualifier();
           pageRank = ByteBuffer.wrap(pageRank_bytes).getFloat();

	}	    // END of your code

            // Use the heap to select the top 20 pages according to page rank
			PageRecord page = new PageRecord(pageDocId, pageUri, pageRank, freq);
			if (topPages.size() < topCount) {
				topPages.offer(page);
			} else {
				PageRecord head = topPages.peek();
				if (page.pageRank > head.pageRank) {
					topPages.poll();
					topPages.offer(page);
				}
			}
			
			pageCount++;
			if (pageCount % 100 == 0) {
				System.out.println("Evaluated " + pageCount + " pages.");
			}
		}
        System.out.println("Evaluated " + pageCount + " pages.");
		dataTable.close();
		indexTable.close();
		prTable.close();
		
		System.out.println("Evaluated " + pageCount + " pages in total. Here are the top 20 pages according to page ranks:");
		Stack<PageRecord> stack = new Stack<PageRecord>();
		while (topPages.size() > 0) {
			stack.push(topPages.poll());
		}
		while (stack.size() > 0) {
			PageRecord page = stack.pop();
			System.out.println("Document ID: " + page.docId + ", URI: " + page.URI + ", page rank: " + page.pageRank + ", word frequency: "
					+ page.termFreq);
		}
	}
	
	public static void getPageSnapshot(String docId) throws Exception {
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable dataTable = new HTable(hbaseConfig, Constants.CW09_DATA_TABLE_BYTES);
		byte[] docIdBytes = Bytes.toBytes(docId);
		Get gDoc = new Get(docIdBytes);
		Result docRow = dataTable.get(gDoc);
		String uri = Bytes.toString(docRow.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES));
		String content = Bytes.toString(docRow.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES));
		System.out.println(uri);
		System.out.println(content);
	}
}
