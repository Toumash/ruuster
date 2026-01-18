use std::time::Duration;

use crossterm::event::{Event as CrosstermEvent, KeyEvent, MouseEvent};
use futures::{FutureExt, StreamExt};

use tokio::sync::mpsc;

/// Terminal events.
#[derive(Clone, Debug)]
pub enum Event {
    Tick,
    Key(KeyEvent),
    Mouse(MouseEvent),
    Resize(u16, u16),
    ChatEvent(crate::update::ChatEvent),
}

/// Terminal event handler.
#[allow(dead_code)]
#[derive(Debug)]
pub struct EventHandler {
    /// Event sender channel.
    pub sender: mpsc::UnboundedSender<Event>,
    /// Event receiver channel.
    receiver: mpsc::UnboundedReceiver<Event>,
    /// Event handler thread.
    handler: tokio::task::JoinHandle<()>,
}

impl EventHandler {
    /// Constructs a new instance of [`EventHandler`].
    pub fn new(tick_rate: u64) -> Self {
        let tick_rate = Duration::from_millis(tick_rate);
        let (sender, receiver) = mpsc::unbounded_channel();
        let _sender = sender.clone();
        let handler = tokio::spawn(async move {
            let mut tick = tokio::time::interval(tick_rate);
            let mut reader = crossterm::event::EventStream::new();
            loop {
                let tick_delay = tick.tick();
                let crossterm_event = reader.next().fuse();
                tokio::select! {
                    _ = _sender.closed() => {
                        break;
                    }
                    _ = tick_delay => {
                        let _ = _sender.send(Event::Tick);
                    }
                    Some(Ok(evt)) = crossterm_event => {
                        match evt {
                            CrosstermEvent::Key(key) => {
                                if key.kind == crossterm::event::KeyEventKind::Press {
                                    let _ = _sender.send(Event::Key(key));
                                }
                            },
                            CrosstermEvent::Mouse(mouse) => {
                                let _ = _sender.send(Event::Mouse(mouse));
                            },
                            CrosstermEvent::Resize(x, y) => {
                                let _ = _sender.send(Event::Resize(x, y));
                            },
                            // Ignore other events but don't break
                            _ => {},
                        }
                    }
                }
            }
        });
        Self {
            sender,
            receiver,
            handler,
        }
    }

    /// Receive the next event from the handler thread.
    ///
    /// This function will always block the current thread if
    /// there is no data available and it's possible for more data to be sent.
    pub async fn next(&mut self) -> Result<Event, Box<dyn std::error::Error>> {
        self.receiver
            .recv()
            .await
            .ok_or(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "This is an IO error",
            )))
    }
}
