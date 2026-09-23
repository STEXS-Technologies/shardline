DROP TRIGGER IF EXISTS shardline_s3_object_reliability_gate ON shardline_s3_objects;
DROP TRIGGER IF EXISTS shardline_oci_tag_reliability_gate ON shardline_oci_tags;
DROP TRIGGER IF EXISTS shardline_hub_ref_reliability_gate ON shardline_hub_refs;
DROP FUNCTION IF EXISTS shardline_require_s3_object_evidence();
DROP FUNCTION IF EXISTS shardline_require_oci_tag_evidence();
DROP FUNCTION IF EXISTS shardline_require_hub_ref_evidence();
