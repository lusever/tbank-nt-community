//! Execution client tests grouped by behavioral surface.

fn bind_test_data_event_sender(
    runtime: &super::TbankExecutionRuntime,
) -> tokio::sync::mpsc::UnboundedReceiver<nautilus_common::messages::DataEvent> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    *runtime
        .data_event_sender
        .lock()
        .expect("data_event_sender lock") = Some(sender.into());
    receiver
}

fn enable_test_command_admission(client: &super::TbankExecutionClient) {
    client
        .runtime
        .lifecycle_active
        .store(true, std::sync::atomic::Ordering::Release);
    client
        .task_owner
        .external_admission_open
        .store(true, std::sync::atomic::Ordering::Release);
    client
        .task_owner
        .mutation_admission_open
        .store(true, std::sync::atomic::Ordering::Release);
}

include!("tests/lifecycle.rs");
include!("tests/submission.rs");
include!("tests/projections.rs");
include!("tests/reports.rs");
include!("tests/translation.rs");
