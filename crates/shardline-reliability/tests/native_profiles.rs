#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_enum_match_arm
)]
use std::collections::BTreeMap;

use shardline_reliability::{
    LifecycleCommand, ResumableLifecycleProfile, ResumableLifecycleState as Session,
    UploadLifecycleProfile, UploadLifecycleState as Upload, native_lifecycle_registry,
    native_resumable_profile, native_upload_profile,
};
use statechronicle::domain::{intent::Operation, resource_state::ResourceState};
use statechronicle::profile::StateProfile;
use statechronicle::profiles::registry::ProfileRules;

#[test]
fn native_upload_semantics_preserve_every_existing_edge() {
    let states = [
        Upload::Created,
        Upload::Storing,
        Upload::Stored,
        Upload::MetadataCommitted,
        Upload::Visible,
        Upload::Failed,
    ];
    // Frozen policy edges; do not compute the expected answer with the reducer.
    let edges = [
        (0, 0),
        (0, 1),
        (0, 5),
        (1, 1),
        (1, 2),
        (1, 5),
        (2, 2),
        (2, 3),
        (2, 5),
        (3, 3),
        (3, 4),
        (3, 5),
        (4, 4),
        (5, 5),
    ];
    for (from, before) in states.iter().enumerate() {
        for (to, after) in states.iter().enumerate() {
            let command = LifecycleCommand::Transition {
                expected: *before,
                next: *after,
            };
            let result = UploadLifecycleProfile.apply(Some(before), &command);
            assert_eq!(
                result.is_ok(),
                edges.contains(&(from, to)),
                "{before:?}->{after:?}"
            );
            if let Ok(state) = result {
                assert_eq!(state, *after);
            }
        }
    }
    assert_eq!(
        UploadLifecycleProfile
            .apply(None, &LifecycleCommand::Initialize)
            .unwrap(),
        Upload::Created
    );
    assert!(
        UploadLifecycleProfile
            .apply(Some(&Upload::Visible), &LifecycleCommand::Initialize)
            .is_err()
    );
    assert!(
        UploadLifecycleProfile
            .apply(
                None,
                &LifecycleCommand::Transition {
                    expected: Upload::Stored,
                    next: Upload::Visible
                }
            )
            .is_err()
    );
    assert!(
        UploadLifecycleProfile
            .apply(
                Some(&Upload::Storing),
                &LifecycleCommand::Transition {
                    expected: Upload::Stored,
                    next: Upload::MetadataCommitted
                }
            )
            .is_err()
    );
}

#[test]
fn native_session_semantics_preserve_recovery_and_terminal_edges() {
    let states = [
        Session::Active,
        Session::Completing,
        Session::Completed,
        Session::Aborted,
        Session::Expired,
    ];
    let edges = [
        (0, 0),
        (0, 1),
        (0, 3),
        (0, 4),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (1, 4),
        (2, 0),
        (2, 2),
        (3, 0),
        (3, 3),
        (4, 0),
        (4, 4),
    ];
    for (from, before) in states.iter().enumerate() {
        for (to, after) in states.iter().enumerate() {
            let result = ResumableLifecycleProfile.apply(
                Some(before),
                &LifecycleCommand::Transition {
                    expected: *before,
                    next: *after,
                },
            );
            assert_eq!(
                result.is_ok(),
                edges.contains(&(from, to)),
                "{before:?}->{after:?}"
            );
        }
    }
    assert_eq!(
        ResumableLifecycleProfile
            .apply(None, &LifecycleCommand::Initialize)
            .unwrap(),
        Session::Active
    );
}

#[test]
fn standard_adapter_enforces_domain_schema_operation_and_bounds() {
    native_lifecycle_registry().unwrap();
    let profile = native_upload_profile();
    let operation = Operation("shardline.upload.initialize".to_owned());
    let command =
        statechronicle::core::canonicalize::canonicalize(&LifecycleCommand::<Upload>::Initialize)
            .unwrap();
    let mut inputs = BTreeMap::from([
        (
            "profile".to_owned(),
            serde_json::json!(UploadLifecycleProfile::DOMAIN_ID),
        ),
        ("schema_version".to_owned(), serde_json::json!(1)),
        ("command".to_owned(), serde_json::json!(command)),
    ]);
    let state = profile.apply(None, &operation, &inputs).unwrap();
    assert_eq!(profile.decode_state(&state).unwrap(), Upload::Created);
    assert!(native_resumable_profile().decode_state(&state).is_err());
    assert!(profile.requires_authority(&operation));
    assert!(
        profile
            .apply(
                None,
                &Operation("shardline.upload.transition".to_owned()),
                &inputs
            )
            .is_err()
    );
    inputs.insert("schema_version".to_owned(), serde_json::json!(2));
    assert!(profile.apply(None, &operation, &inputs).is_err());
    inputs.insert("schema_version".to_owned(), serde_json::json!(1));
    inputs.insert("command".to_owned(), serde_json::json!(vec![0; 33]));
    assert!(profile.apply(None, &operation, &inputs).is_err());
    let custom_state = match state {
        ResourceState::Custom(custom) => Some(custom),
        _ => None,
    };
    let mut custom = custom_state.expect("custom state");
    custom.schema_version = 2;
    assert!(
        profile
            .decode_state(&ResourceState::Custom(custom))
            .is_err()
    );
}

#[test]
fn native_schema_one_commands_keep_their_canonical_wire_tags() {
    let encode = |command: LifecycleCommand<Upload>| {
        statechronicle::core::canonicalize::canonicalize(&command).unwrap()
    };
    assert_eq!(encode(LifecycleCommand::Initialize), vec![0]);
    assert_eq!(
        encode(LifecycleCommand::Transition {
            expected: Upload::Storing,
            next: Upload::Stored
        }),
        vec![1, 1, 2]
    );
    assert_eq!(
        statechronicle::core::canonicalize::canonicalize(&LifecycleCommand::Transition {
            expected: Session::Completed,
            next: Session::Active
        })
        .unwrap(),
        vec![1, 2, 0]
    );
}
