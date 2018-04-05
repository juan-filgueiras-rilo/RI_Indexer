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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;

public class ReutersIndexer {
	
	private static final String ADDED_INDEX_DIR = "\\addedIndex";
	private enum IndexOperation {
		NONE,
		CREATE,
		PROCESS;
	}
	private enum Ord {
		ALF,
		TF_DEC,
		DF_DEC;
	}
	private static IndexOperation OP = IndexOperation.NONE;
	private static void setOpIfNone(IndexOperation op) {
		if(ReutersIndexer.OP.equals(IndexOperation.NONE)) {
			ReutersIndexer.OP = op;
		}
	}
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> o1.getValue().compareTo(o2.getValue()));

        Map<K, V> result = new LinkedHashMap<>();
        for (Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }
	private ReutersIndexer() {}
	
	/** Index all text files under a directory. 
	 **/
	public static void main(String[] args) {
		String usage = "java org.apache.lucene.demo.IndexFiles" + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		
		//String indexPath = "D:\\RI\\index";
		String indexPath = "D:\\UNI\\3º\\Recuperación de la Información\\indexIn";
		//String docsPath = "D:\\RI\\reuters21578";
		String docsPath = "D:\\UNI\\3º\\Recuperación de la Información\\Práctica 2\\reuters21578";
		//String indexOut = "D:\\RI\\index\\summaries";
		String indexOut = "D:\\UNI\\3º\\Recuperación de la Información\\indexOut";
		OpenMode modo = OpenMode.CREATE_OR_APPEND;
		boolean multithread = false;
		boolean addindexes = false;

		boolean bestIdfTerms = false;
		boolean tfPos = false;
		boolean termstfpos1 = false;
		boolean deldocsterm = false;
		boolean deldocsquery = false;
		boolean summaries = false;
		String termName = "said";
		String fieldName = "body";
		String query = "";
		int bestN = 10;
		int docID = 0;
		Ord ord = Ord.ALF;
		
		for(int i=0;i<args.length;i++) {
			switch(args[i]) {
			//Creacion indice
			case("-index"):
				setOpIfNone(IndexOperation.CREATE);
				if (args.length-1 >= i+1 && isValidPath(args[i+1])) {
					indexPath = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -index.\n " + usage);
					System.exit(-1);
				}
			case("-coll"):
				setOpIfNone(IndexOperation.CREATE);
				if (args.length-1 >= i+1 && isValidPath(args[i+1])) {
					docsPath = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -coll.\n " + usage);
					System.exit(-1);
				}
			case("-openmode"):	
				setOpIfNone(IndexOperation.CREATE);
				if(args.length-1 >= i+1) {
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
				} else {
					System.err.println("Wrong option -openmode.\n " + usage);
					System.exit(-1);
				}
			case("-multithread"):
				setOpIfNone(IndexOperation.CREATE);
				multithread = true;
				break;
			case("-addindexes"):
				setOpIfNone(IndexOperation.CREATE);
				addindexes = true;
				break;
			//Procesado indice
			case("-indexin"):
				setOpIfNone(IndexOperation.PROCESS);
				if (args.length-1 >= i+1 && isValidPath(args[i+1])) {
					indexPath = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -indexin.\n " + usage);
					System.exit(-1);
				}
			case("-indexout"):
				setOpIfNone(IndexOperation.PROCESS);
				if (args.length-1 >= i+1 && isValidPath(args[i+1])) {
					indexOut = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -indexout.\n " + usage);
					System.exit(-1);
				}
			case("-best_idfterms"):
				setOpIfNone(IndexOperation.PROCESS);
				bestIdfTerms = true;
				if(args.length-1 >= i+2) {
					fieldName = args[i+1];
					try {
						bestN = Integer.parseInt(args[i+2]);
					} catch (NumberFormatException e) {
						System.err.println("Error while parsing the number of best n_terms\n " + usage);
						System.exit(-1);
					}
					i+=2;
					break;
				} else {
					System.err.println("Wrong option -best_idfterms.\n " + usage);
					System.exit(-1);
				}
			case("-tfpos"):
				setOpIfNone(IndexOperation.PROCESS);
				tfPos = true;
				if(args.length-1 >= i+1) {
					fieldName = args[i+1];
					if(args.length-1 >= i+2) {
						termName = args[i+2];
					}
					i+=2;
					break;
				} else {
					System.err.println("Wrong option -tfpos.\n " + usage);
					System.exit(-1);
				}
			case("-termstfpos1"):
				setOpIfNone(IndexOperation.PROCESS);
				termstfpos1 = true;
				if(args.length-1 >= i+3) {
					try {
						docID = Integer.parseInt(args[i+1]);
					} catch (NumberFormatException e) {
						System.err.println("Error while parsing the given Lucene docID\n " + usage);
						System.exit(-1);
					}
					fieldName = args[i+2];
					switch(Integer.parseInt(args[i+3])) {
					case(0):
						ord = Ord.ALF;
					break;
					case(1):
						ord = Ord.TF_DEC;
					break;
					case(2):
						ord = Ord.DF_DEC;
					break;
					default: System.err.println("Wrong option -termstfpos1 <ord>.\n " + usage);
						System.exit(-1);
					}
					i+=3;
					break;
				} else {
					System.err.println("Wrong option -termstfpos1.\n " + usage);
					System.exit(-1);
				}
			case("-deldocsterm"):
				setOpIfNone(IndexOperation.PROCESS);
				if(args.length-1 >= i+2){
					fieldName = args[++i];
					termName = args[++i];
					deldocsterm = true;
				} else {
					System.err.println("Wrong option -deldocsterm.\n " + usage);
					System.exit(-1);
				}
				break;
			case("-deldocsquery"):
				setOpIfNone(IndexOperation.PROCESS);
				if(args.length-1 >= i+1){
					query = args[++i];
					deldocsquery = true;
				} else {
					System.err.println("Wrong option -deldocsquery.\n " + usage);
					System.exit(-1);
				}
				break;
			case("-summaries"):
				setOpIfNone(IndexOperation.PROCESS);
				if(args.length-1 >= i){
					summaries = true;
				} else {
					System.err.println("Wrong option -summaries.\n " + usage);
					System.exit(-1);
				}
				break;
			}
		}

		Date start = new Date();
		try {
			if(ReutersIndexer.OP.equals(IndexOperation.CREATE)) {
				final Path docDir = Paths.get(docsPath);
				if (!Files.isReadable(docDir)) {
					System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
					System.exit(1);
				}
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
				
				IndexWriter indexWriter = new IndexWriter(dir, iwc);
				
				if(multithread) {
					end = startThreads(docDir, indexPath, modo, addindexes, indexWriter);
				}  else {
					indexDocs(indexWriter, docDir);
				}
				indexWriter.close();
	
				if(end == null) {
					end = new Date();
				}
				System.out.println(end.getTime() - start.getTime() + " total milliseconds");
			} else if (ReutersIndexer.OP.equals(IndexOperation.PROCESS)) {
				try {
					
					Directory dir;
					dir = FSDirectory.open(Paths.get(indexPath));
					
					if (deldocsterm){
						deleteDocsByTerm(fieldName, termName, dir);
					}

					DirectoryReader indexReader;
					indexReader = DirectoryReader.open(dir);
					if (deldocsquery){
						deleteDocsByQuery(query, dir, indexReader);
					}
					if(bestIdfTerms) {
						calculateBestIdfTerms(fieldName, bestN, indexReader);
					}
					if(tfPos) {
						createTfPosList(fieldName, termName, indexReader);
					}
					if(termstfpos1) {
						List<TermData> termsList = createTermTfPosList(docID, fieldName, indexReader);
						switch (ord) {
							case ALF:
								break;
							case DF_DEC:
								termsList.sort(TermData::compareByDocFreq);
								break;
							case TF_DEC:
								termsList.sort(TermData::compareByTermFreq);
								break;
						}
						for(TermData t: termsList) {
							System.out.println(t.toString());
							System.out.println("-----------------------------------------");
						}
					}
					if(summaries) {
						createIndexWithSummaries(indexReader, indexOut);
					}
					indexReader.close();
				} catch (CorruptIndexException | ParseException e1) {
					System.err.println("Graceful message: exception " + e1);
					e1.printStackTrace();
				}
			}
		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() +
					"\n with message: " + e.getMessage());
		}
	}

	private static void createIndexWithSummaries(DirectoryReader indexReader, String indexOut) throws IOException {
		
		Directory outDir = FSDirectory.open(Paths.get(indexOut));
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		iwc.setRAMBufferSizeMB(512.0);

		IndexWriter mainWriter = new IndexWriter(outDir, iwc);
		String tempPath = indexOut+"\\tmp";
		Directory RAMDir = FSDirectory.open(Paths.get(indexOut));
		Analyzer subAnalyzer = new StandardAnalyzer();
		IndexWriterConfig subIwc = new IndexWriterConfig(subAnalyzer);
		iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		iwc.setRAMBufferSizeMB(512.0);
		
		for(int i=0; i<indexReader.numDocs(); i++) {
		
			IndexWriter subWriter = new IndexWriter(RAMDir, subIwc);
			Document doc = indexReader.document(i);
			IndexableField body = doc.getField("body");
	        SentenceTokenizer sentenceTokenizer= new SentenceTokenizer();
	        sentenceTokenizer.setText(body.stringValue());
	        String[] sentences = sentenceTokenizer.getSentences();
	        int sno = 0;
	        
	        for (String s:sentences) {
	            Document tempDoc = new Document();
	            tempDoc.add(new TextField("sentences", s, Field.Store.YES));
	            //doc.setBoost(computeDeboost(pno, sno));
	            subWriter.addDocument(tempDoc);
	            sno++;
	        }

			if(sno == 0) {
				Field field = new TextField("summary", "", Field.Store.YES);
				doc.add(field);
				subWriter.addDocument(doc);
				subWriter.close();
				continue;
			}
	        QueryParser parser = new QueryParser("sentence", new StandardAnalyzer());
	        try {
	        	int n = 2;
				Query q = parser.parse(doc.getField("title").stringValue());
				Directory tempDir = FSDirectory.open(Paths.get(tempPath));
		        DirectoryReader subIndexReader = DirectoryReader.open(tempDir);
		        IndexSearcher indexSearcher = new IndexSearcher(subIndexReader);
		        if(sno == 1) {
		        	n = 1;
		        }
		        TopDocs td = indexSearcher.search(q, n);
		        ScoreDoc[] sd = td.scoreDocs;
		        String summary = "";
		        for(ScoreDoc d: sd) {
		        	Document dd = subIndexReader.document(d.doc);
		        	summary += dd.getField("sentence").stringValue();
		        }
		        
				Field field = new TextField("summary", summary, Field.Store.YES);
				doc.add(field);
				mainWriter.addDocument(doc);
				subWriter.close();

			} catch (org.apache.lucene.queryparser.classic.ParseException e) {
				System.err.println(e.getMessage());
				System.exit(-1);
			}
	        
		}
		mainWriter.close();
	}
	
	private static void deleteDocsByTerm(String fieldName, String termName, Directory dir) throws IOException {

		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.APPEND);
		iwc.setRAMBufferSizeMB(512.0);

		IndexWriter writer = new IndexWriter(dir, iwc);
		Term term = new Term(fieldName, termName);

		writer.deleteDocuments(term);
		writer.close();
		System.out.println("Done.");
	}

	private static void deleteDocsByQuery(String query, Directory dir, DirectoryReader indexReader) throws IOException, ParseException {

		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.APPEND);
		iwc.setRAMBufferSizeMB(512.0);
		
		String[] fields = {"topics","title","dateline","body","date"};
		
		QueryParser parser = new MultiFieldQueryParser(fields, analyzer);
		Query q;
		try {
			q = parser.parse(query);
			IndexWriter writer = new IndexWriter(dir, iwc);
			writer.deleteDocuments(q);
			writer.close();
			System.out.println("Done.");
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	private static List<TermData> createTermTfPosList(int docID, String fieldName, DirectoryReader indexReader) throws IOException {

		Document doc = null;
		Terms terms = null;
		BytesRef term = null;
		IndexableField path = null;
		List<Integer> positionList;
		List<TermData> termsList = new ArrayList<>();

		doc = indexReader.document(docID);
		path = doc.getField("path");
		terms = indexReader.getTermVector(docID, fieldName);

		if(terms != null) {
			TermsEnum termsEnum = terms.iterator();
			PostingsEnum postings = null;
			while ((termsEnum.next() != null)) {
				positionList = new ArrayList<>();
				term = termsEnum.term();
				postings = termsEnum.postings(postings, PostingsEnum.ALL);
				postings.nextDoc();
				for (int i=postings.freq(); i>0; i--) {
					int pos = postings.nextPosition();
					positionList.add(pos);
				}
				termsList.add(new TermData(term.utf8ToString(),(int)termsEnum.totalTermFreq(),indexReader.docFreq(new Term(fieldName,term)),positionList));
			}
		} else {
			for (final LeafReaderContext leaf : indexReader.leaves()) {

				try (LeafReader leafReader = leaf.reader()) {

					terms = leafReader.fields().terms(fieldName);
					TermsEnum termsEnum = terms.iterator();
					while ((termsEnum.next() != null)) {
						final String tt = termsEnum.term().utf8ToString();
						final PostingsEnum postings = leafReader.postings(new Term(fieldName, tt), PostingsEnum.ALL);
						int whereDoc;
						while((whereDoc = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
							if(whereDoc == docID) {
								break;
							}
							postings.nextDoc();
						}
						if (whereDoc == docID) {
							positionList = new ArrayList<>();
							for (int i=postings.freq(); i>0; i--) {
								int pos = postings.nextPosition();
								positionList.add(pos);
							}
							termsList.add(new TermData(tt,postings.freq(),termsEnum.docFreq(),positionList));
						}
					}
				}
			}
		}
		System.out.println("Document ID nº " + docID + ":");
		System.out.println("Field: "+ fieldName);
		System.out.println("Path: " + path.stringValue());
		System.out.println("Terms");
		System.out.println("-----------------------------------------");
		return termsList;
	}
	
	private static void createTfPosList(String fieldName, String termName, DirectoryReader indexReader) throws IOException {
		
		Document doc = null;
		Term term = new Term(fieldName, termName);
		int docID;
		
		for (final LeafReaderContext leaf : indexReader.leaves()) {
			
			try (LeafReader leafReader = leaf.reader()) {
				
				final PostingsEnum postings = leafReader.postings(term, PostingsEnum.ALL);

				while((docID = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
					doc = leafReader.document(docID);
					System.out.println("Document ID: " + postings.docID());
					System.out.println("Indexed from: " + doc.getField("path").stringValue());
					int freq = postings.freq();
					System.out.println("Term Frequency: " + freq);
					//Posiciones
					System.out.print("Term at positions: ");
					for (int i=freq; i>0; i--) {
						int pos = postings.nextPosition();
						System.out.print(pos + " ");
					}
					System.out.println("\n");
					//df termino
				}
				System.out.println("Total DocFreq of term '" + termName + "': " + leafReader.docFreq(term));
			}
		}
	}
	
	private static void calculateBestIdfTerms(String fieldName, int bestN, DirectoryReader indexReader) throws IOException {
		
		int docCount = indexReader.numDocs();
		TFIDFSimilarity similarity = new ClassicSimilarity();
		System.out.println("Size of  indexReader.leaves() = " + indexReader.leaves().size());
		for (final LeafReaderContext leaf : indexReader.leaves()) {
			// Print leaf number (starting from zero)
			System.out.println("We are in the leaf number " + leaf.ord);

			// Create an AtomicReader for each leaf
			// (using, again, Java 7 try-with-resources syntax)
			try (LeafReader leafReader = leaf.reader()) {
			
				// Get the fields contained in the current segment/leaf
				final Fields fields = leafReader.fields();
				System.out.println(
						"Numero de campos devuelto por leafReader.fields() = " + fields.size());
				System.out.println("Field = " + fieldName);
				final Terms terms = fields.terms(fieldName);
				final TermsEnum termsEnum = terms.iterator();
				Map<String, Float> termList = new LinkedHashMap<>();
				while ((termsEnum.next() != null)) {
					final String tt = termsEnum.term().utf8ToString();
					final long docFreq = termsEnum.docFreq();
					final float idf = similarity.idf(docFreq, docCount);
					termList.put(tt, idf);
				}
				termList = sortByValue(termList);
				System.out.println(docCount);
				Set<Entry<String, Float>> setList = termList.entrySet();
				for (Entry<String, Float> bestTerm : setList) {
					if (bestN == 0)
						break;
	
					System.out.println(bestTerm.getKey() + ", " + bestTerm.getValue());
					bestN--;
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}
	
	private static Date startThreads(final Path docDir, String indexPath, OpenMode modo, boolean addindexes, IndexWriter writer) {
		//final int numCores = Runtime.getRuntime().availableProcessors();
		//final ExecutorService executor = Executors.newFixedThreadPool(numCores);
		final ExecutorService executor = Executors.newCachedThreadPool();
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
			// NOTE: if you want to maximize search performance,
			// you can optionally call forceMerge here.  This can be
			// a terribly costly operation, so generally it's only
			// worth it when your index is relatively static (ie
			// you're done adding documents to it):
			//
			//writer.forceMerge(1);
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
		
		FieldType t = new FieldType();
		t.setTokenized(true);
		t.setStored(true);
		t.setStoreTermVectors(true);
		t.setStoreTermVectorOffsets(true);
		t.setStoreTermVectorPositions(true);
		t.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		t.freeze();
		
		try (InputStream stream = Files.newInputStream(file)) {
			List<List<String>> parsedContent = Reuters21578Parser.parseString(fileToBuffer(stream));
			for (List<String> parsedDoc:parsedContent) {
//			for (Iterator<List<String>> iterator = parsedContent.iterator(); iterator.hasNext();) {

//				List<String> parsedDoc = iterator.next();
				// make a new, empty document
				Document doc = new Document();
				
				Field pathSgm = new StringField("path", file.toString(), Field.Store.YES);
				doc.add(pathSgm);
				
				Field hostname = new StringField("hostname", System.getProperty("user.name"), Field.Store.NO);
				doc.add(hostname);
				
				Field thread = new StringField("thread", Thread.currentThread().getName(), Field.Store.NO);
				doc.add(thread);
				
				Field topics = new TextField("topics", parsedDoc.get(2), Field.Store.YES);
				doc.add(topics);
				
				Field title = new TextField("title", parsedDoc.get(0), Field.Store.YES);
				doc.add(title);
				
				Field dateLine = new StringField("dateline", parsedDoc.get(4), Field.Store.YES);
				doc.add(dateLine);
				
				Field body = new Field("body", parsedDoc.get(1), t);
				doc.add(body);
				
				//26-FEB-1987 15:01:01.79
				SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMMM-yyyy HH:mm:ss.SS", Locale.ENGLISH);
				Date date = dateFormat.parse(parsedDoc.get(3));
				String dateText = DateTools.dateToString(date, Resolution.SECOND);
				Field dateField = new StringField("date", dateText, Field.Store.YES);
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
