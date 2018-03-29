package es.udc.fic.ri.mri_indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/*
 * Query parser syntax:
 * http://lucene.apache.org/core/6_3_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description
*/

public class ReutersIndexer {
	
	private static final String ADDED_INDEX_DIR = "\\addedIndex";

	private ReutersIndexer() {}
	
	/** Index all text files under a directory. */
	public static void main(String[] args) {
		String usage = "java org.apache.lucene.demo.IndexFiles" + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		String indexPath = "D:\\RI\\index";
		//String indexPath = "D:\\UNI\\3º\\Recuperación de la Información\\2018";
		String docsPath = "D:\\RI\\reuters21578";
		//String docsPath = "D:\\UNI\\3º\\Recuperación de la Información\\Práctica 2\\reuters21578";
		OpenMode modo = OpenMode.CREATE_OR_APPEND;
		boolean multithread = false;
		boolean addindexes = false;

		for(int i=0;i<args.length;i++) {
			switch(args[i]) {
			case("-index"):
				if (isValidPath(args[i+1])) {
					indexPath = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -index.\n " + usage);
					System.exit(-1);
				}
			case("-coll"):
				if (isValidPath(args[i+1])) {
					docsPath = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -coll.\n " + usage);
					System.exit(-1);
				}
			case("-openmode"):				
				switch(args[i+1]) {
				case "create": modo = OpenMode.CREATE;
					break;
				case "append": modo = OpenMode.APPEND;
					break;
				case "create_or_append": modo = OpenMode.CREATE_OR_APPEND;
					break;
				default: System.err.println("Wrong option -openmode.\n " + usage);
					System.exit(-1);
				}
				i++;
				break;
			case("-multithread"):
				multithread = true;
				break;
			case("-addindexes"):
				addindexes = true;
				break;
			}
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}

		Date start = new Date();
		try {
			Date end = null;
			//Añado ruta de addIndexes para poder crear el writer principal antes de trabajar en las subcarpetas de mismo nivel.
			if(multithread && addindexes)
				indexPath = indexPath.concat(ReutersIndexer.ADDED_INDEX_DIR);
			System.out.println("Indexing to directory '" + indexPath + "'...");

			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(modo);
			iwc.setRAMBufferSizeMB(512.0);
			
			IndexWriter writer = new IndexWriter(dir, iwc);
			
			if(multithread) {
				end = startThreads(docDir, indexPath, modo, addindexes, writer);
			}  else {
				indexDocs(writer, docDir);
			}
			writer.close();

			if(end == null) {
				end = new Date();
			}
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() +
					"\n with message: " + e.getMessage());
		}
	}

	private static Date startThreads(final Path docDir, String indexPath, OpenMode modo, boolean addindexes, IndexWriter writer) {
		//final int numCores = Runtime.getRuntime().availableProcessors();
		//final ExecutorService executor = Executors.newFixedThreadPool(numCores);
		final ExecutorService executor = Executors.newCachedThreadPool();
		List<Directory> dirs = new ArrayList<>();
		Date end = null;
		//Elimino la ruta sobre la que cree el indexWriter principal, ya no lo necesito.
		if(addindexes)
				indexPath = indexPath.replace(ReutersIndexer.ADDED_INDEX_DIR, "\\");
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir)) {
			//final ExecutorService executor = Executors.newFixedThreadPool(numDirs);
			
			/* We process each subfolder in a new thread. */
			for (final Path subDocDir : directoryStream) {
				if (Files.isDirectory(subDocDir)) {
					final Runnable worker;
					if(addindexes) {
						Directory subIndexPath = FSDirectory.open(Paths.get(indexPath,
							docDir.relativize(subDocDir).toString()));
						dirs.add(subIndexPath);
						worker = new WorkerThread(subDocDir, subIndexPath, modo, writer);
					}
					else 
						worker = new WorkerThread(subDocDir, writer);
					/*
					 * Send the thread to the ThreadPool. It will be processed
					 * eventually.
					 */
					executor.execute(worker);
				}
			}
			/*
			 * Close the ThreadPool; no more jobs will be accepted, but all the
			 * previously submitted jobs will be processed.
			 */
			executor.shutdown();

			/* Wait up to 1 hour to finish all the previously submitted jobs */
			try {
				executor.awaitTermination(5, TimeUnit.MINUTES);
			} catch (final InterruptedException e) {
				e.printStackTrace();
				System.exit(-2);
			}
			System.out.println("Finished all threads");
			end = new Date();
//			if (addindexes) {
//				for (Directory d: dirs)
//					writer.addIndexes(d);
//			}
			// NOTE: if you want to maximize search performance,
			// you can optionally call forceMerge here.  This can be
			// a terribly costly operation, so generally it's only
			// worth it when your index is relatively static (ie
			// you're done adding documents to it):
			//
			writer.forceMerge(1);
		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() +
					"\n with message: " + e.getMessage());
		}
		return end;	
	}
	/**
	 * Indexes the given file using the given writer, or if a directory is given,
	 * recurses over files and directories found under the given directory.
	 * 
	 * NOTE: This method indexes one document per input file.  This is slow.  For good
	 * throughput, put multiple documents into your input file(s).  An example of this is
	 * in the benchmark module, which can create "line doc" files, one document per line,
	 * using the
	 * <a href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
	 * >WriteLineDocTask</a>.
	 *  
	 * @param writer Writer to the index where the given file/dir info will be stored
	 * @param path The file to index, or the directory to recurse into to find files to index
	 * @throws IOException If there is a low-level I/O error
	 */
	static void indexDocs(final IndexWriter writer, Path path) throws IOException {
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
						if (file.toString().endsWith(".sgm"))
							indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
					} catch (IOException ignore) {
						// don't index files that can't be read.
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		}
	}

	/** Indexes a single document */
	static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			List<List<String>> parsedContent = Reuters21578Parser.parseString(fileToBuffer(stream));
			for (List<String> parsedDoc:parsedContent) {
//			for (Iterator<List<String>> iterator = parsedContent.iterator(); iterator.hasNext();) {

//				List<String> parsedDoc = iterator.next();
				// make a new, empty document
				Document doc = new Document();
				
				Field pathSgm = new StringField("path", file.toString(), Field.Store.NO);
				doc.add(pathSgm);
				
				Field hostname = new StringField("hostname", System.getProperty("user.name"), Field.Store.NO);
				doc.add(hostname);
				
				Field thread = new StringField("thread", Thread.currentThread().getName(), Field.Store.NO);
				doc.add(thread);
				
				Field topics = new TextField("topics", parsedDoc.get(2), Field.Store.YES);
				doc.add(topics);
				
				Field title = new TextField("title", parsedDoc.get(0), Field.Store.NO);
				doc.add(title);
				
				Field dateLine = new StringField("dateline", parsedDoc.get(4), Field.Store.YES);
				doc.add(dateLine);
				
				Field body = new TextField("body", parsedDoc.get(1), Field.Store.YES);
				doc.add(body);
				
				//26-FEB-1987 15:01:01.79
				SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMMM-yyyy HH:mm:ss.SS", Locale.ENGLISH);
				Date date = dateFormat.parse(parsedDoc.get(3));
				String dateText = DateTools.dateToString(date, Resolution.SECOND);
				Field dateField = new StringField("date", dateText, Field.Store.NO);
				doc.add(dateField);
				
				if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
					// New index, so we just add the document (no old document can be there):
					writer.addDocument(doc);
				} else {
					// Existing index (an old copy of this document may have been indexed) so
					// we use updateDocument instead to replace the old one matching the exact
					// path, if present:
					writer.updateDocument(new Term("path", file.toString()), doc);
				}
			}
			if (writer.getConfig().getOpenMode() == OpenMode.CREATE)
				System.out.println("adding " + file);
			else
				System.out.println("updating " + file);
				
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	public static StringBuffer fileToBuffer(InputStream is) throws IOException {
		StringBuffer buffer = new StringBuffer();
		InputStreamReader isr = null;

		try {
			isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;

			while ((line = br.readLine()) != null) {
				buffer.append(line + "\n");
			}
		} finally {
			if (is != null) {
				is.close();
			}
			if (isr != null) {
				isr.close();
			}
		}

		return buffer;
	}
	
    public static boolean isValidPath(String path) {

        try {
            Paths.get(path);
        } catch (InvalidPathException | NullPointerException ex) {
            return false;
        }
        return true;
    }
}
