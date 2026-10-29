//! Read-only S3 ownership and ACL responses for the fixed, ACL-disabled model.
//!
//! Every S3 bucket is repository-scoped. Shardline's namespace ID is the
//! stable opaque principal for both the bucket and its objects. ACL changes
//! are rejected; authorization remains controlled by repository capabilities.

use shardline_s3_adapter::xml_escape;

/// Returns the bucket-owner-enforced ownership configuration.
#[must_use]
pub(super) fn ownership_controls_xml() -> String {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>".to_owned()
}

/// Returns the default full-control ACL used when ACLs are disabled.
#[must_use]
pub(super) fn access_control_policy_xml(owner_id: &str) -> String {
    let owner_id = xml_escape(owner_id);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>{owner_id}</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{owner_id}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_controls_report_acl_disabled_mode() {
        let xml = ownership_controls_xml();
        assert!(xml.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>"));
    }

    #[test]
    fn acl_owner_is_escaped_and_has_full_control() {
        let xml = access_control_policy_xml("tenant<&");
        assert!(xml.contains("<Owner><ID>tenant&lt;&amp;</ID></Owner>"));
        assert!(xml.contains("<Permission>FULL_CONTROL</Permission>"));
        assert_eq!(xml.matches("tenant&lt;&amp;").count(), 2);
    }
}
