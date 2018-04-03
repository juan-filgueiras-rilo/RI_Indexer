package es.udc.fic.ri.mri_indexer;

import java.util.List;

public class TermData implements Comparable<TermData>{
    private String name;
    private int frequency;
    private int docFrequency;
    private List<Integer> positions;

    public TermData(String name, int frequency, int docFrequency, List<Integer> positions) {
        this.name = name;
        this.frequency = frequency;
        this.docFrequency = docFrequency;
        this.positions = positions;
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

    @Override
    public int compareTo(TermData o) {
        return this.name.compareTo(o.getName());
    }

    public int compareByTermFreq(TermData o) {
        return this.frequency-o.getFrequency();
    }

    public int compareByDocFreq(TermData o) {
        return this.docFrequency-o.getDocFrequency();
    }
}
