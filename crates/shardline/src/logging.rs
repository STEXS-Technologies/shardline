//! Keep optional task diagnostics out of machine-readable command stdout.
use std::io::{self, Write};
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone, Copy)]
pub(crate) struct CommandLogWriter;

pub(crate) enum CommandLogOutput {
    Application(io::Stdout),
    Tracking(io::Stderr),
}

impl<'writer> MakeWriter<'writer> for CommandLogWriter {
    type Writer = CommandLogOutput;

    fn make_writer(&'writer self) -> Self::Writer {
        CommandLogOutput::Application(io::stdout())
    }

    fn make_writer_for(&'writer self, metadata: &tracing::Metadata<'_>) -> Self::Writer {
        let target = metadata.target();
        if target == "shardline::maintenance"
            || target.starts_with("shardline::maintenance::")
            || target == "shardline_server::task_tracking"
            || target.starts_with("shardline_server::task_tracking::")
        {
            CommandLogOutput::Tracking(io::stderr())
        } else {
            self.make_writer()
        }
    }
}

impl Write for CommandLogOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Application(writer) => writer.write(bytes),
            Self::Tracking(writer) => writer.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Application(writer) => writer.flush(),
            Self::Tracking(writer) => writer.flush(),
        }
    }
}
