package es.udc.fic.ri.mri_indexer;

import java.nio.file.Path;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;

public class WorkerThread implements Runnable {
	
	private final Path subDocDir;
	private IndexWriter writer;
	private final boolean closeable;
	
	public WorkerThread(final Path path, final Directory subIndexPath, OpenMode modo) {
		
		this.subDocDir = path;
		this.closeable = true;
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(modo);
		iwc.setRAMBufferSizeMB(512.0);
		try {
			this.writer = new IndexWriter(subIndexPath, iwc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public WorkerThread(final Path path, IndexWriter writer) {
		
		this.closeable = false;
		this.subDocDir = path;
		this.writer = writer;
	}
	
	@Override
	public void run() {
		System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
				Thread.currentThread().getName(), subDocDir));
		try {
			ReutersIndexer.indexDocs(this.writer, this.subDocDir);
			if(closeable) {
				this.writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
