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
import java.util.Iterator;
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
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;

public class ReutersIndexer {
	
	private static FieldType getCustomFieldType() {
		FieldType t = new FieldType();
		t.setTokenized(true);
		t.setStored(true);
		t.setStoreTermVectors(true);
		t.setStoreTermVectorOffsets(true);
		t.setStoreTermVectorPositions(true);
		t.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		t.freeze();
		return t;
	}
	private static final String ADDED_INDEX_DIR = "\\addedIndex";
	private enum IndexOperation {
		NONE,
		CREATE,
		PROCESS;
	}
	private enum Ord {
		ALF,
		TF_DEC,
		DF_DEC,
		TF_IDF_DEC;
	}
	private static IndexOperation OP = IndexOperation.NONE;
	private static boolean setOpIfNone(IndexOperation op) {
		if(ReutersIndexer.OP.equals(IndexOperation.NONE)) {
			ReutersIndexer.OP = op;
			return true;
		}
		if(ReutersIndexer.OP.equals(op)) {
			return true;
		}
		return false;
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
		String indexPath = "D:\\RI\\index";
		//String indexPath = "D:\\UNI\\3º\\Recuperación de la Información\\indexIn";
		String docsPath = "D:\\RI\\reuters21578";
		//String docsPath = "D:\\UNI\\3º\\Recuperación de la Información\\Práctica 2\\reuters21578";
		String indexOut = "D:\\RI\\summaries";
		//String indexOut = "D:\\UNI\\3º\\Recuperación de la Información\\indexOut";
		OpenMode modo = OpenMode.CREATE_OR_APPEND;
		boolean multithread = false;
		boolean addindexes = false;

		boolean bestIdfTerms = false;
		boolean tfPos = false;
		boolean termstfpos1 = false;
		boolean termstfpos2 = false;
		boolean deldocsterm = false;
		boolean deldocsquery = false;
		
		boolean summaries = false;
		String termName = "said";
		String fieldName = "body";
		String query = "cocoa";
		String pathName = "D:\\RI\\reuters21578\\0-4\\reut2-000.sgm";
		String newID = "12222";
		int bestN = 10;
		int docID = 0;
		int numTotalThreads = 0;
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
					System.err.println("Wrong option -index.\n ");
					System.exit(-1);
				}
			case("-coll"):
				setOpIfNone(IndexOperation.CREATE);
				if (args.length-1 >= i+1 && isValidPath(args[i+1])) {
					docsPath = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -coll.\n ");
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
					default: System.err.println("Wrong option -openmode.\n ");
						System.exit(-1);
					}
					i++;
					break;
				} else {
					System.err.println("Missing arg for -openmode.\n ");
					System.exit(-1);
				}
			case("-multithread"):
				try {
					numTotalThreads = Integer.parseInt(args[i + 1]);
					if(!setOpIfNone(IndexOperation.PROCESS)) {
						System.err.println("Wrong option -multithread.\n ");
						System.exit(-1);
					}
				} catch (IndexOutOfBoundsException | NumberFormatException e1){
					if(!setOpIfNone(IndexOperation.CREATE)) {
						System.err.println("Wrong option -multithread.\n ");
						System.exit(-1);
					}
				}
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
					System.err.println("Wrong option -indexin.\n ");
					System.exit(-1);
				}
			case("-indexout"):
				setOpIfNone(IndexOperation.PROCESS);
				if (args.length-1 >= i+1 && isValidPath(args[i+1])) {
					indexOut = args[i+1];
					i++;
					break;
				} else {
					System.err.println("Wrong option -indexout.\n ");
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
						System.err.println("Error while parsing number of best n_terms\n ");
						System.exit(-1);
					}
					i+=2;
					break;
				} else {
					System.err.println("Wrong option -best_idfterms.\n ");
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
					System.err.println("Wrong option -tfpos.\n ");
					System.exit(-1);
				}
			case("-termstfpos1"):
				setOpIfNone(IndexOperation.PROCESS);
				termstfpos1 = true;
				if(args.length-1 >= i+3) {
					try {
						docID = Integer.parseInt(args[i+1]);
					} catch (NumberFormatException e) {
						System.err.println("Error while parsing the given Lucene docID\n ");
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
					case(3):
						ord = Ord.TF_IDF_DEC;
						break;
					default: System.err.println("Wrong option -termstfpos1 <ord>.\n ");
						System.exit(-1);
					}
					i+=3;
					break;
				} else {
					System.err.println("Wrong option -termstfpos1.\n ");
					System.exit(-1);
				}
			case("-termstfpos2"):
				setOpIfNone(IndexOperation.PROCESS);
				termstfpos2 = true;
				if(args.length-1 >= i+4) {
					pathName = args[++i];
					newID = args[++i];
					fieldName = args[++i];
					switch(Integer.parseInt(args[++i])) {
					case(0):
						ord = Ord.ALF;
						break;
					case(1):
						ord = Ord.TF_DEC;
						break;
					case(2):
						ord = Ord.DF_DEC;
						break;
					case(3):
						ord = Ord.TF_IDF_DEC;
						break;
					default: System.err.println("Wrong option -termstfpos2 <ord>.\n ");
						System.exit(-1);
					}
					break;
				} else {
					System.err.println("Wrong option -termstfpos2.\n ");
					System.exit(-1);
				}	
			case("-deldocsterm"):
				setOpIfNone(IndexOperation.PROCESS);
				if(args.length-1 >= i+2){
					fieldName = args[++i];
					termName = args[++i];
					deldocsterm = true;
				} else {
					System.err.println("Wrong option -deldocsterm.\n ");
					System.exit(-1);
				}
				break;
			case("-deldocsquery"):
				setOpIfNone(IndexOperation.PROCESS);
				if(args.length-1 >= i+1){
					query = args[++i];
					deldocsquery = true;
				} else {
					System.err.println("Wrong option -deldocsquery.\n ");
					System.exit(-1);
				}
				break;
			case("-summaries"):
				setOpIfNone(IndexOperation.PROCESS);
				if(args.length-1 >= i){
					summaries = true;
				} else {
					System.err.println("Wrong option -summaries.\n ");
					System.exit(-1);
				}
				break;
			}
		}

		Date start = new Date();
		Date end = null;
		try {
			if(ReutersIndexer.OP.equals(IndexOperation.CREATE)) {
				final Path docDir = Paths.get(docsPath);
				if (!Files.isReadable(docDir)) {
					System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
					System.exit(1);
				}
				
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
	
			} else if (ReutersIndexer.OP.equals(IndexOperation.PROCESS)) {
				try {
					Directory dir;
					dir = FSDirectory.open(Paths.get(indexPath));
					
					if (deldocsterm){
						deleteDocsByTerm(fieldName, termName, dir);
					}					
					if (deldocsquery) {
						deleteDocsByQuery(query, dir);
					}
					if(bestIdfTerms) {
						calculateBestIdfTerms(fieldName, bestN, dir);
					}
					if(tfPos) {
						createTfPosList(fieldName, termName, dir);
					}
					if(termstfpos1) {
						List<TermData> termsList1 = createTermTfPosList1(docID, fieldName, dir, true);
						switch (ord) {
							case ALF:
								break;
							case DF_DEC:
								termsList1.sort(TermData::compareByDocFreq);
								break;
							case TF_DEC:
								termsList1.sort(TermData::compareByTermFreq);
								break;
							case TF_IDF_DEC:
								termsList1.sort(TermData::compareByTfPerIdf);
								break;
						}
						int count = 5;
						for(TermData t: termsList1) {
							if(ord.equals(Ord.TF_IDF_DEC)) {
								count--;
							}
							if (count == 0) 
								break;
							System.out.println(t.toString());
							System.out.println("-----------------------------------------");
						}
					}
					if(termstfpos2) {
						List<TermData> termsList2 = createTermTfPosList2(newID, pathName, fieldName, dir);
						switch (ord) {
							case ALF:
								break;
							case DF_DEC:
								termsList2.sort(TermData::compareByDocFreq);
								break;
							case TF_DEC:
								termsList2.sort(TermData::compareByTermFreq);
								break;
							case TF_IDF_DEC:
								termsList2.sort(TermData::compareByTfPerIdf);
								break;	
						}
						for(TermData t: termsList2) {
							System.out.println(t.toString());
							System.out.println("-----------------------------------------");
						}
					}
					if(summaries) {
						if(multithread) {
							startThreads(indexOut, dir, numTotalThreads);
						} else {
							createIndexWithSummaries(dir, indexOut);
						}
					}
				} catch (CorruptIndexException | ParseException e) {
					System.err.println("Graceful message: exception " + e);
					e.printStackTrace();
				}
			}
			if(end == null) {
				end = new Date();
			}
			System.out.println(end.getTime()/1000 - start.getTime()/1000 + " total milliseconds");
		} catch (IOException e) {
			System.err.println("Caught a " + e.getClass() + " with message: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private static void newSummarize(int numDoc, DirectoryReader indexReader, IndexWriter mainWriter) throws IOException {

		Directory RAMDir = new RAMDirectory();
		Analyzer subAnalyzer = new StandardAnalyzer();
		IndexWriterConfig subIwc = new IndexWriterConfig(subAnalyzer);
		IndexWriter subWriter = new IndexWriter(RAMDir, subIwc);

		Document doc = indexReader.document(numDoc);
		Document toAddDoc = new Document();

		List <IndexableField> fields = doc.getFields();
		for(IndexableField field: fields) {
			String content = field.stringValue();
			Field newField;
			if(field.name().equals("path") || field.name().equals("dateline")) {
				newField = new StringField(field.name(), content, Field.Store.YES);
			} else if (field.name().equals("topics") || field.name().equals("title") || field.name().equals("oldid") || field.name().equals("newid")) {
				newField = new TextField(field.name(), content, Field.Store.YES);
			} else {
				newField = new Field(field.name(), content, ReutersIndexer.getCustomFieldType());
			}
			
			toAddDoc.add(newField);
		}
		
		Field thread = new StringField("thread", Thread.currentThread().getName(), Field.Store.NO);
		toAddDoc.add(thread);
		
		Field hostname = new StringField("hostname", System.getProperty("user.name"), Field.Store.NO);
		toAddDoc.add(hostname);
		
		IndexableField body = doc.getField("body");
		String[] sentences = body.stringValue().split("\\.");
		int sno = 0;

		for (String s : sentences) {
			Document tempDoc = new Document();
			tempDoc.add(new TextField("sentence", s + ". ", Field.Store.YES));
			subWriter.addDocument(tempDoc);
			sno++;
		}

		QueryParser parser = new QueryParser("sentence", new StandardAnalyzer());

		int n = sno;
		String summary = "";
		IndexableField titleField = doc.getField("title");
		String topTitleTerms = "";
		
		//termsList = createTermTfPosList1(numDoc, "title", indexReader.directory(), false);
		List<TermData> termsList = new ArrayList<>();
		Terms terms = indexReader.getTermVector(numDoc, "title");
		if(terms != null) {
			TermsEnum termsEnum = terms.iterator();
			PostingsEnum postings = null;
			while ((termsEnum.next() != null)) {
				BytesRef term = termsEnum.term();
				postings = termsEnum.postings(postings, PostingsEnum.ALL);
				postings.nextDoc();
				termsList.add(new TermData(term.utf8ToString(), (int)termsEnum.totalTermFreq(), indexReader.docFreq(new Term("title", term)), null/*, indexReader.numDocs()*/));
			}
		} else {
			terms = MultiFields.getTerms(indexReader, "title");
			TermsEnum termsEnum = terms.iterator();
			while ((termsEnum.next() != null)) {
				final String tt = termsEnum.term().utf8ToString();
				final PostingsEnum postings = MultiFields.getTermPositionsEnum(indexReader, "title", new Term("title", tt).bytes(), PostingsEnum.ALL);//(new Term(fieldName, tt), PostingsEnum.ALL);
				int whereDoc;
				
				while((whereDoc = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
					if(whereDoc >= numDoc) {
						break;
					}
					postings.nextDoc();
				}
				if (whereDoc == numDoc) {
					termsList.add(new TermData(tt, postings.freq(), termsEnum.docFreq(), null/*,indexReader.numDocs()*/));
				}
			}
		}
		if ((titleField != null) && (!titleField.stringValue().isEmpty()) && (!termsList.isEmpty())) {
	
			String titleQuery = " "; // = bestTerms(titleField.stringValue(),indexReader);
			topTitleTerms = "Top 10 title terms: \n";
			//Obtener tres mejores terminos titulo por tfxidf.			
			//termsList = createTermTfPosList1(numDoc, "title", indexReader.directory());
			termsList.sort(TermData::compareByTfPerIdf);
			int count = 10;
			for (TermData term : termsList) {
				if (count > 7)
					titleQuery += term.getName() + " ";
				topTitleTerms += "Term : " + term.getName() + " | tf: " + term.getFrequency() + " | idf: " + term.getIdf() + " | tfxidf: " + term.getTfPerIdf() + "\n";
				count--;
				if (count == 0)
					break;
			}
			//Query con tres mejores terminos titulo
			Query q = parser.createPhraseQuery("sentence", titleQuery);
			DirectoryReader subIndexReader = DirectoryReader.open(subWriter);
			IndexSearcher indexSearcher = new IndexSearcher(subIndexReader);
			if (sno >= 3) {
				n = 3;
			}
			TopDocs td = indexSearcher.search(q, n);
			ScoreDoc[] sd = td.scoreDocs;

			for (ScoreDoc d : sd) {
				Document dd = subIndexReader.document(d.doc);
				summary += dd.getField("sentence").stringValue();
			}
			if (sd.length == 0) {
				//3 primeras frases body
				for (int i = 0; i < sno; i++) {
					if (i == 3) break;
					summary += sentences[i];
				}
			}
		} else {
			//3 primeras frases body
			for (int i = 0; i < sno; i++) {
				if (i == 3) break;
				summary += sentences[i];
			}
		}
		Field summaryField = new TextField("summary", summary, Field.Store.YES);
		
		//Mejores terminos TOP 10

		Field topTermsField = new TextField("topterms", topTitleTerms, Field.Store.YES);
		
		toAddDoc.add(summaryField);
		toAddDoc.add(topTermsField);
		mainWriter.addDocument(toAddDoc);
		subWriter.close();
	}
	
	private static void summarize(int numDoc, DirectoryReader indexReader, IndexWriter mainWriter) throws IOException {

		Directory RAMDir = new RAMDirectory();
		Analyzer subAnalyzer = new StandardAnalyzer();
		IndexWriterConfig subIwc = new IndexWriterConfig(subAnalyzer);
		IndexWriter subWriter = new IndexWriter(RAMDir, subIwc);

		Document doc = indexReader.document(numDoc);
		Document toAddDoc = new Document();

		List <IndexableField> fields = doc.getFields();
		for(IndexableField field: fields) {
			String content = field.stringValue();
			Field newField;
			if(field.name().equals("path") || field.name().equals("dateline")) {
				newField = new StringField(field.name(), content, Field.Store.YES);
			} else if (field.name().equals("topics") || field.name().equals("title") || field.name().equals("oldid") || field.name().equals("newid")) {
				newField = new TextField(field.name(), content, Field.Store.YES);
			} else {
				newField = new Field(field.name(), content, ReutersIndexer.getCustomFieldType());
			}
			
			toAddDoc.add(newField);
		}
		
		Field thread = new StringField("thread", Thread.currentThread().getName(), Field.Store.NO);
		toAddDoc.add(thread);
		
		Field hostname = new StringField("hostname", System.getProperty("user.name"), Field.Store.NO);
		toAddDoc.add(hostname);
		
		IndexableField body = doc.getField("body");
		String[] sentences = body.stringValue().split("\\.");
		int sno = 0;

		for (String s : sentences) {
			Document tempDoc = new Document();
			tempDoc.add(new TextField("sentence", s + ". ", Field.Store.YES));
			subWriter.addDocument(tempDoc);
			sno++;
		}

//		if (sno == 0) {
//			Field field = new TextField("summary", null, Field.Store.YES);
//			toAddDoc.add(field);
//			mainWriter.addDocument(toAddDoc);
//			continue;
//		}

		QueryParser parser = new QueryParser("sentence", new StandardAnalyzer());

		int n = sno;
		String summary = "";
		IndexableField titleField = doc.getField("title");

		if ((titleField != null) && (!titleField.stringValue().isEmpty())) {

			Query q = parser.createPhraseQuery("sentence", titleField.stringValue());
			DirectoryReader subIndexReader = DirectoryReader.open(subWriter);
			IndexSearcher indexSearcher = new IndexSearcher(subIndexReader);
			if (sno >= 2) {
				n = 2;
			}
			TopDocs td = indexSearcher.search(q, n);
			ScoreDoc[] sd = td.scoreDocs;

			for (ScoreDoc d : sd) {
				Document dd = subIndexReader.document(d.doc);
				summary += dd.getField("sentence").stringValue();
			}
			if (sd.length == 0) {
				//2 primeras frases body
				for (int i = 0; i < sno; i++) {
					if (i == 2) break;
					summary += sentences[i];
				}
			}
		} else {
			//2 primeras frases body
			for (int i = 0; i < sno; i++) {
				if (i == 2) break;
				summary += sentences[i];
			}
		}
		Field field = new TextField("summary", summary, Field.Store.YES);
		toAddDoc.add(field);
		mainWriter.addDocument(toAddDoc);
		subWriter.close();
	}

	public static void startThreads(String indexOut, Directory dir, int numTotalThreads) throws IOException {
		
		int i;
		final ExecutorService executor = Executors.newFixedThreadPool(numTotalThreads);
		Directory outDir = FSDirectory.open(Paths.get(indexOut));
		DirectoryReader indexReader = DirectoryReader.open(dir);
        Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.CREATE);
		iwc.setRAMBufferSizeMB(512.0);
		IndexWriter indexOutWriter = new IndexWriter(outDir, iwc);
		
		for (i=0; i < numTotalThreads; i++){
			final Runnable worker = new SummaryThread(indexReader, indexOutWriter, i, numTotalThreads);
			executor.execute(worker);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(8, TimeUnit.HOURS);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(-2);
		}
		System.out.println("Finished all threads");
		indexOutWriter.close();
		indexReader.close();
	}
	
	public static void createIndexWithSummaries(Directory dir, String indexOut) throws IOException {
		
		DirectoryReader indexReader;
		indexReader = DirectoryReader.open(dir);
		
		Directory outDir = FSDirectory.open(Paths.get(indexOut));
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.CREATE);
		iwc.setRAMBufferSizeMB(512.0);

		IndexWriter mainWriter = new IndexWriter(outDir, iwc);
		
		for(int numDoc=0; numDoc<indexReader.numDocs(); numDoc++) {
			System.out.println("Indexing doc. no: " + numDoc);
			//summarize(numDoc, indexReader, mainWriter);
			newSummarize(numDoc, indexReader, mainWriter);
		}
		mainWriter.close();
	}
	
	public static void createIndexWithSummaries(SummaryThread thread) throws IOException {
		
		for(int numDoc = thread.getThreadNumber(); numDoc < thread.getIndexReader().numDocs(); numDoc+=thread.getNumTotalThreads()) {
			System.out.println("Indexing doc. no: " + numDoc + " with thread nº" + thread.getThreadNumber());
			//summarize(numDoc, thread.getIndexReader(), thread.getIndexOutWriter());
			newSummarize(numDoc, thread.getIndexReader(), thread.getIndexOutWriter());
		}
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

	private static void deleteDocsByQuery(String query, Directory dir) throws IOException, ParseException {

		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.APPEND);
		iwc.setRAMBufferSizeMB(512.0);
		
		String[] fields = {"topics","title","dateline","body","date","oldid","newid"};
		
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

	private static List<TermData> createTermTfPosList2(String pathName, String newID, String fieldName, Directory dir) throws IOException {
		
		DirectoryReader indexReader;
		indexReader = DirectoryReader.open(dir);
		Document doc = null;
		List<TermData> termsList = null;
		
		for(int docID=0; docID < indexReader.numDocs(); docID++) {
			doc = indexReader.document(docID);
			final IndexableField pathField = doc.getField("path");
			final IndexableField newIDField = doc.getField("newid");
			if (pathField.stringValue().equals(pathName) && newIDField.stringValue().equals(newID)) {
				termsList = createTermTfPosList1(docID, fieldName, dir, true);
			}
			
		}
		return termsList;
	}
	
	private static List<TermData> createTermTfPosList1(int docID, String fieldName, Directory dir, boolean show) throws IOException {

		Document doc = null;
		Terms terms = null;
		BytesRef term = null;
		IndexableField path = null;
		IndexableField oldID = null;
		IndexableField newID = null;
		List<Integer> positionList;
		List<TermData> termsList = new ArrayList<>();

		DirectoryReader indexReader;
		indexReader = DirectoryReader.open(dir);
		
		doc = indexReader.document(docID);
		path = doc.getField("path");
		oldID = doc.getField("oldid");
		newID = doc.getField("newid");
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
				termsList.add(new TermData(term.utf8ToString(), (int)termsEnum.totalTermFreq(), indexReader.docFreq(new Term(fieldName,term)), positionList/*, indexReader.numDocs()*/));
			}
		} else {
			terms = MultiFields.getTerms(indexReader, fieldName);
			TermsEnum termsEnum = terms.iterator();
			while ((termsEnum.next() != null)) {
				final String tt = termsEnum.term().utf8ToString();
				final PostingsEnum postings = MultiFields.getTermPositionsEnum(indexReader, fieldName, new Term(fieldName, tt).bytes(), PostingsEnum.ALL);//(new Term(fieldName, tt), PostingsEnum.ALL);
				int whereDoc;
				
				while((whereDoc = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
					if(whereDoc >= docID) {
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
					termsList.add(new TermData(tt, postings.freq(), termsEnum.docFreq(), positionList/*,indexReader.numDocs()*/));
				}
			}
		}
		if(show) {
			System.out.println("Document ID nº " + docID + ":");
			System.out.println("Field: "+ fieldName + "\nTitle: " + doc.getField("title").stringValue());
			System.out.println("Path: " + path.stringValue());
			System.out.println("OldID: " + oldID.stringValue());
			System.out.println("NewID: " + newID.stringValue());
			System.out.println("Terms");
			System.out.println("-----------------------------------------");
		}
		return termsList;
	}
	
	private static void createTfPosList(String fieldName, String termName, Directory dir) throws IOException {
		
		DirectoryReader indexReader;
		indexReader = DirectoryReader.open(dir);
		Document doc = null;
		Term term = new Term(fieldName, termName);
		int docID;
		
		final PostingsEnum postings = MultiFields.getTermPositionsEnum(indexReader, fieldName, term.bytes(), PostingsEnum.ALL);
		try {
			while((docID = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
				doc = indexReader.document(docID);
				System.out.println("Document ID: " + postings.docID());
				System.out.println("Indexed from: " + doc.getField("path").stringValue());
				System.out.println("Doc OldID: " + doc.getField("oldid").stringValue());
				System.out.println("Doc NewID: " + doc.getField("newid").stringValue());
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
			System.out.println("Total DocFreq of term '" + termName + "': " + indexReader.docFreq(term));
		} catch(NullPointerException e) {
			System.err.println("Term " + termName + " not found!");
			e.printStackTrace();
		}
	}
	
	private static void calculateBestIdfTerms(String fieldName, int bestN, Directory dir) throws IOException {
		
		DirectoryReader indexReader;
		indexReader = DirectoryReader.open(dir);
		int docCount = indexReader.numDocs();
		TFIDFSimilarity similarity = new ClassicSimilarity();
		
		for (final LeafReaderContext leaf : indexReader.leaves()) {
			try (LeafReader leafReader = leaf.reader()) {
				final Fields fields = leafReader.fields();				
				final Terms terms = fields.terms(fieldName);
				final TermsEnum termsEnum = terms.iterator();
				Map<String, Float> termList = new LinkedHashMap<>();
				while ((termsEnum.next() != null)) {
					final String tt = termsEnum.term().utf8ToString();
					final long docFreq = termsEnum.docFreq();
					//final float idf = similarity.idf(docFreq, docCount);
					final float idf = new Float(Math.log((float)docCount/docFreq));
					termList.put(tt, idf);
				}
				termList = sortByValue(termList);
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
				executor.awaitTermination(2, TimeUnit.HOURS);
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
				Document doc = new Document();
				
				Field pathSgm = new StringField("path", file.toString(), Field.Store.YES);
				doc.add(pathSgm);
				
				Field hostname = new StringField("hostname", System.getProperty("user.name"), Field.Store.NO);
				doc.add(hostname);
				
				Field thread = new StringField("thread", Thread.currentThread().getName(), Field.Store.NO);
				doc.add(thread);
				
				Field topics = new TextField("topics", parsedDoc.get(2), Field.Store.YES);
				doc.add(topics);
				
				Field title = new Field("title", parsedDoc.get(0), t);
				doc.add(title);
				
				Field dateLine = new StringField("dateline", parsedDoc.get(4), Field.Store.YES);
				doc.add(dateLine);
				
				Field body = new Field("body", parsedDoc.get(1), t);
				doc.add(body);
				
				Field oldID = new StringField("oldid", parsedDoc.get(5), Field.Store.YES);
				//Field oldID = new LongPoint("oldid", Long.parseLong(parsedDoc.get(5)));
				doc.add(oldID);
				
				Field newID = new StringField("newid", parsedDoc.get(6), Field.Store.YES);
				//Field newID = new LongPoint("newid", Long.parseLong(parsedDoc.get(6)));
				doc.add(newID);
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
