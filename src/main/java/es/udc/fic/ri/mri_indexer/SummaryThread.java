package es.udc.fic.ri.mri_indexer;

import org.apache.lucene.store.Directory;

import java.io.IOException;

public class SummaryThread implements Runnable{

    final Directory dir;
    final String indexOut;
    final int numTotalThreads;
    final int i;

    public SummaryThread(Directory dir, String indexOut, int i, int numTotalThreads) {
        this.dir=dir;
        this.indexOut=indexOut;
        this.i=i;
        this.numTotalThreads=numTotalThreads;
    }

    @Override
    public void run() {
        try {
            ReutersIndexer.createIndexWithSummaries(dir, indexOut,false,0,numTotalThreads);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
