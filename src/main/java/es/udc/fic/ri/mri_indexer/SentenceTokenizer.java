package es.udc.fic.ri.mri_indexer;

public class SentenceTokenizer {

    private String text;

    public SentenceTokenizer()  {
    }

    public void setText(String text) {
        this.text = text;
    }

    public String[] getSentences() {
        return text.split("\n");
    }

}

