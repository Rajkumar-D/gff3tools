package uk.ac.ebi.embl.gfftools.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.embl.gfftools.model.FFAnnotation;

@Repository
public class NativeGffRepository {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Transactional
    public void updateAnnotation(FFAnnotation contigAnnotation, FFAnnotation scaffoldAnnotation){
        String query="update DROP_ENA_6374_GFF3_2024 set contig_annotation_size_bytes = ?, scaffold_annotation_size_bytes = ?, contig_feature_count= ?, scaffold_feature_count=?,gff3_status=? where primaryacc# = ? ";
        jdbcTemplate.update(query, contigAnnotation.getTotalAnnotationSize(), scaffoldAnnotation.getTotalAnnotationSize(),contigAnnotation.getTotalFeatureCount(),scaffoldAnnotation.getTotalFeatureCount(),"CALCULATED",contigAnnotation.getPrimaryAccession());

    }
}
