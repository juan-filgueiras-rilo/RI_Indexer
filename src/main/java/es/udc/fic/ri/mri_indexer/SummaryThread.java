package es.udc.fic.ri.mri_indexer;

import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;

public class SummaryThread implements Runnable {

    public DirectoryReader getIndexReader() {
		return indexReader;
	}

	public IndexWriter getIndexOutWriter() {
		return indexOutWriter;
	}

	public int getNumTotalThreads() {
		return numTotalThreads;
	}

	public int getThreadNumber() {
		return threadNumber;
	}

	private final DirectoryReader indexReader;
	private final IndexWriter indexOutWriter;
	private final int numTotalThreads;
	private final int threadNumber;

    public SummaryThread(DirectoryReader indexReader, IndexWriter indexOutWriter, int threadNumber, int numTotalThreads) throws IOException {
    	
        this.indexReader = indexReader;
        this.indexOutWriter = indexOutWriter;
        this.threadNumber = threadNumber;
        this.numTotalThreads = numTotalThreads;
    }

    @Override
    public void run() {
    	
        try {
            ReutersIndexer.createIndexWithSummaries(this);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
