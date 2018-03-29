package es.udc.fic.ri.mri_indexer;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;

public class WorkerThread implements Runnable {
	
	//Creación índice
	private final Path subDocDir;
	private Directory subIndexPath;
	private IndexWriter mainWriter;
	private IndexWriter threadWriter;
	private final boolean hasThreadWriter;
	
	public WorkerThread(final Path path, final Directory subIndexPath, OpenMode modo, IndexWriter writer) {
		
		this.subDocDir = path;
		this.hasThreadWriter = true;
		this.mainWriter = writer;
		this.subIndexPath = subIndexPath;
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(modo);
		iwc.setRAMBufferSizeMB(512.0);
		try {
			this.threadWriter = new IndexWriter(subIndexPath, iwc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public WorkerThread(final Path path, IndexWriter writer) {
		
		this.hasThreadWriter = false;
		this.subDocDir = path;
		this.mainWriter = writer;
		this.threadWriter = null;
	}
	
	@Override
	public void run() {
		System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
				Thread.currentThread().getName(), subDocDir));
		try {
			IndexWriter currentWriter;
			if(hasThreadWriter) {
				currentWriter = this.threadWriter;
			} else {
				currentWriter = this.mainWriter;
			}
			ReutersIndexer.indexDocs(currentWriter, this.subDocDir);
			
			if(hasThreadWriter) {
				this.threadWriter.close();
				this.mainWriter.addIndexes(this.subIndexPath);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
