package uk.ac.ebi.embl.gfftools.model;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
@Getter
public class FFAnnotation {
    Long totalFeatureCount;
    Long totalAnnotationSize;
    String primaryAccession;
    public FFAnnotation() {}
    public FFAnnotation(String primaryAccession, Long totalFeatureCount, Long totalAnnotationSize) {
        this.primaryAccession = primaryAccession;
        this.totalFeatureCount = totalFeatureCount;
        this.totalAnnotationSize = totalAnnotationSize;
    }

    @Override
    public String toString() {
        return "totalFeatureCount: "+totalFeatureCount+"\n" +
                "totalAnnotationSize: "+totalAnnotationSize;
    }
}
