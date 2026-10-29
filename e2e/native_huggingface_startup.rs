//! Observe startup failure without detaching a server on failed readiness.
use super::*;

pub(super) async fn wait(
    url: &str,
    server: &mut JoinHandle<Result<(), ServerError>>,
    role: &str,
) -> Result<(), TestError> {
    tokio::select! {
        outcome = &mut *server => {
            // Server errors can contain credentials or backend URLs. Report
            // lifecycle classification, keeping raw diagnostics private.
            let reason = match outcome {
                Ok(Ok(())) => "exited successfully before readiness",
                Ok(Err(_)) => "failed before readiness",
                Err(error) if error.is_panic() => "panicked before readiness",
                Err(_) => "was cancelled before readiness",
            };
            Err(format!("{role} fixture {reason}").into())
        }
        ready = wait_for_health(url) => {
            if ready.is_err() {
                server.abort();
            }
            ready.map_err(|error| format!("{role} fixture readiness failed: {error}").into())
        }
    }
}

#[tokio::test]
async fn startup_observes_terminated_server_without_waiting_for_probe_deadline() {
    let mut server = tokio::spawn(async { Ok(()) });
    let error = wait("http://127.0.0.1:1", &mut server, "test")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exited successfully before readiness")
    );
    assert!(server.is_finished());
}
