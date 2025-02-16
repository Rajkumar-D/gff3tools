package uk.ac.ebi.embl.gfftools.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import uk.ac.ebi.embl.gfftools.model.FFAnnotation;

@Repository
public class NativeGffRepository {

    @Autowired
    JdbcTemplate jdbcTemplate;

    public void updateAnnotation(FFAnnotation contigAnnotation, FFAnnotation scaffoldAnnotation){
        String query="update DROP_ENA_6374_GFF3_2015 set contig_annotation_size_bytes = ?, scaffold_annotation_size_bytes = ?, contig_feature_count= ?, scaffold_feature_count=? where primaryacc# = ? ";
        jdbcTemplate.update(query, contigAnnotation.getTotalAnnotationSize(), scaffoldAnnotation.getTotalAnnotationSize(),contigAnnotation.getTotalFeatureCount(),scaffoldAnnotation.getTotalFeatureCount(),contigAnnotation.getPrimaryAccession());
    }
}
