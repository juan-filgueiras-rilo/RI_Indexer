package es.udc.fic.ri.mri_indexer;

import java.util.List;

public class TermData implements Comparable<TermData>{
    private String name;
    private int frequency;
    private int docFrequency;
    private List<Integer> positions;
//    private int totalNumDocs;

    public TermData(String name, int frequency, int docFrequency, List<Integer> positions/*, int totalNumDocs*/) {
        this.name = name;
        this.frequency = frequency;
        this.docFrequency = docFrequency;
        this.positions = positions;
//        this.totalNumDocs = totalNumDocs;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public int getDocFrequency() {
        return docFrequency;
    }

    public void setDocFrequency(int docFrequency) {
        this.docFrequency = docFrequency;
    }

    public List<Integer> getPositions() {
        return positions;
    }

    public void setPositions(List<Integer> positions) {
        this.positions = positions;
    }

    public float getIdf() {
    	return ((float)1/this.docFrequency);
    }
    
    public float getTfPerIdf() {
    	return (float) this.frequency*((float)1/this.docFrequency);
    }
    
//    public void setTotalNumDocs(int totalNumDocs) {
//    	this.totalNumDocs = totalNumDocs;
//    }
//    
//    public int getTotalNumDocs() {
//    	return this.totalNumDocs;
//    }
    
    @Override
    public int compareTo(TermData o) {
        return this.name.compareTo(o.getName());
    }

    public int compareByTermFreq(TermData o) {
        return o.getFrequency()-this.frequency;
    }

    public int compareByDocFreq(TermData o) {
        return o.getDocFrequency()-this.docFrequency;
    }
    
    public int compareByTfPerIdf(TermData o) {
    	return new Float((float)o.getFrequency()*((float)1/o.getDocFrequency())).compareTo((float)this.frequency*((float)1/this.docFrequency));
        //return (o.getFrequency()*(o.getTotalNumDocs()/o.getDocFrequency()))-(this.frequency*(this.totalNumDocs/this.docFrequency));
    }
    
    @Override
    public String toString() {
    	String positionString = "";
    	for(Integer pos: positions) {
    		positionString+=pos.toString() + " ";
    	}
    	return "Term: " + this.name + "\n" + "Term frequency: " + this.frequency + "\n" + "Term at positions: " + positionString + "\n" + "Term DocFrequency: " + this.docFrequency;
    }
}
