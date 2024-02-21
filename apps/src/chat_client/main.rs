mod app;
mod event;
mod request;
mod tui;
mod ui;
mod update;

use app::App;
use event::{Event, EventHandler};
use protos::ruuster_client::RuusterClient;
use ratatui::{backend::CrosstermBackend, Terminal};
use tui::Tui;
use update::{dispatch_update, update_message_list};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RuusterClient::connect("http://127.0.0.1:50051").await?;
    let mut app = App::new();

    // Initialize the terminal user interface.
    let backend = CrosstermBackend::new(std::io::stderr());
    let terminal = Terminal::new(backend)?;
    let events = EventHandler::new(500);
    let mut tui = Tui::new(terminal, events);
    tui.enter()?;

    while !app.is_done() {
        // Render the user interface.
        tui.draw(&mut app)?;
        // Handle events.
        let _ = match tui.events.next()? {
            Event::Tick => update_message_list(&mut client, &mut app).await,
            Event::Key(key_event) => dispatch_update(&mut client, &mut app, key_event).await,
            Event::Mouse(_) => Ok(()),
            Event::Resize(_, _) => Ok(())
        };
    }

    tui.exit()?;
    Ok(())
}
