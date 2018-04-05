package es.udc.fic.ri.mri_indexer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class SentenceTokenizer {

    private String text;

    public SentenceTokenizer()  {
    }

    public void setText(String text) {
        this.text = text;
    }

    public String[] getSentences() {
        return text.split("\\.");
    }

    public static void main (String[] args){
        System.out.println(args[0]);
        String query=args[0];
        SentenceTokenizer sentenceTokenizer= new SentenceTokenizer();
        sentenceTokenizer.setText(args[1]);
        String[] sentences = sentenceTokenizer.getSentences();
        int sno = 0;
        for (String s:sentences){
            //Document doc = new Document();
            //doc.add(new Field("text", sentence, Store.YES, Index.ANALYZED));
            //doc.setBoost(computeDeboost(pno, sno));
            //writer.addDocument(doc);
            System.out.println(s);
            sno++;
        }
        System.out.println(sno);
    }
}


