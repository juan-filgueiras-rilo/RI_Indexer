package es.udc.fic.ri.mri_indexer;

import java.nio.file.Path;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

public class WorkerThread implements Runnable {
	
	private final Path path;
	private final IndexWriter writer;

	public WorkerThread(final Path path, final IndexWriter writer) {
		this.path = path;
		this.writer = writer;
	}

	@Override
	public void run() {
		System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
				Thread.currentThread().getName(), path));
		try {
			ReutersIndexer.indexDocs(writer, path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
