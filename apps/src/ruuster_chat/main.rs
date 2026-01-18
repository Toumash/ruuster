mod app;
mod event;
mod input_handler;
mod model;
mod ruuster;
mod update;
mod view;

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io;

use crate::app::App;
use crate::event::{Event, EventHandler};
use crate::input_handler::{handle_key_events, handle_mouse_events};
use crate::model::RunningState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app
    let mut app = App::new();
    let tick_rate = 250;
    let mut event_handler = EventHandler::new(tick_rate);
    app.update(crate::update::ChatEvent::SetSender(
        event_handler.sender.clone(),
    ))
    .await?;

    // Main loop
    while app.model.running_state != RunningState::Done {
        terminal.draw(|f| view::render(&app, f))?;

        let event = event_handler.next().await?;
        match event {
            Event::Key(key_event) => {
                handle_key_events(key_event, &mut app).await?;
            }
            Event::Mouse(mouse_event) => {
                handle_mouse_events(mouse_event, &mut app);
            }
            Event::ChatEvent(chat_event) => {
                app.update(chat_event).await?;
            }
            Event::Tick => {}
            _ => {}
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
