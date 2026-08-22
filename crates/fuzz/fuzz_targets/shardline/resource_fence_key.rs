#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::{ResourceLockDomain, ResourceLockKey};

fuzz_target!(|data: (&str, &str, &str)| {
    let (first, second, third) = data;

    let oci = ResourceLockKey::oci_repository(first, second);
    let repeated_oci = ResourceLockKey::oci_repository(first, second);
    assert_eq!(
        oci, repeated_oci,
        "canonical construction must be deterministic"
    );
    assert_eq!(oci.domain(), ResourceLockDomain::OciRepository);

    let provider = ResourceLockKey::provider_repository(first, second, third);
    let repeated_provider = ResourceLockKey::provider_repository(first, second, third);
    assert_eq!(
        provider, repeated_provider,
        "canonical construction must be deterministic"
    );
    assert_eq!(provider.domain(), ResourceLockDomain::ProviderRepository);

    assert_ne!(
        oci, provider,
        "different mutable-resource domains must never alias"
    );
});
