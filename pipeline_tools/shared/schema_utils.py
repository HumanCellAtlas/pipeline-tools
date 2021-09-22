SCHEMAS = {
    "FILE_DESCRIPTOR" : {
        "describedBy" : "https://schema.humancellatlas.org/system/2.0.0/file_descriptor",
        "schema_type" : "file_descriptor",
        "schema_version" : "2.0.0"
    },
    "ANALYSIS_FILE" : {
        "describedBy" : "https://schema.humancellatlas.org/type/file/6.3.0/analysis_file",
        "schema_type" : "file",
        "schema_version" : "6.3.0",
        "file_source" : "DCP/2 Analysis",
        "loom_content_description": {
            "text": "DCP/2-generated matrix",
            "ontology": "data:3917",
            "ontology_label": "Count Matrix"
        },
        "bam_content_description" : {
            "text" : "DCP/2-sequence alignment",
            "ontology" : "data:1383",
            "ontology_label" : "Nucleic acid sequence alignment"
        },
        "bai_content_description" : {
            "text" : "DCP/2-sequence alignment index",
            "ontology" : "data:3210",
            "ontology_label" : "An index of a genome sequence"
        },
    },
    "ANALYSIS_PROCESS" : {
        "describedBy" : "https://schema.humancellatlas.org/type/process/analysis/12.0.0/analysis_process",
        "schema_type" : "process",
        "schema_version" : "12.0.0",
        "optimus_input_fields": ["r1_fastq", "r2_fastq", "i1_fastq", "whitelist", "input_id", "tar_star_reference", "annotations_gtf", "ref_genome_fasta", "chemistry"],
        "ss2_input_fields": ["fastq1", "fastq2", "gene_ref_flat", "genome_ref_fasta", "hisat2_ref_index", "hisat2_ref_name", "hisat2_ref_trans_name", "rrna_intervals", "rsem_ref_index", "stranded", "output_name"]
    },
    "ANALYSIS_PROTOCOL" : {
        "describedBy" : "https://schema.humancellatlas.org/type/protocol/analysis/9.1.0/analysis_protocol",
        "schema_type" : "protocol",
        "schema_version" : "9.1.0"
    },
    "METADATA_REFERENCE" : {
        "describedBy" : "https://schema.humancellatlas.org/type/file/3.2.0/reference_file",
        "schema_type" : "file",
        "schema_version" : "3.2.0"
    },
    "LINKS" : {
        "describedBy" : "https://schema.humancellatlas.org/system/2.1.1/links",
        "schema_type" : "links",
        "schema_version" : "2.1.1"
    },
}
