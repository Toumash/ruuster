use crate::app::App;
use crate::model::{ChatMessage, RunningState};
use crate::update::ChatEvent;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

pub async fn handle_key_events(
    key_event: KeyEvent,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    // Handle Ctrl+C globally to quit
    if key_event.modifiers.contains(KeyModifiers::CONTROL) && key_event.code == KeyCode::Char('c') {
        app.update(ChatEvent::Quit).await?;
        return Ok(());
    }

    match app.model.running_state {
        RunningState::StartView => handle_start_view_input(key_event, app).await,
        RunningState::RoomListView => handle_room_list_view_input(key_event, app).await,
        RunningState::ChatView => handle_chat_view_input(key_event, app).await,
        RunningState::Done => Ok(()),
    }
}

async fn handle_start_view_input(
    key_event: KeyEvent,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    match key_event.code {
        KeyCode::Enter => {
            let addr = if app.model.input.is_empty() {
                "http://127.0.0.1:50051".to_string()
            } else {
                app.model.input.clone()
            };
            app.model.input.clear();
            app.update(ChatEvent::ConnectToServer(addr)).await?;
        }
        KeyCode::Char(c) => {
            app.model.input.push(c);
        }
        KeyCode::Backspace => {
            app.model.input.pop();
        }
        KeyCode::Esc => {
            app.update(ChatEvent::Quit).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_room_list_view_input(
    key_event: KeyEvent,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    match key_event.code {
        KeyCode::Enter => {
            let room_name = app.model.input.clone();
            if !room_name.is_empty() {
                app.model.input.clear();
                // For simplicity, nick is just "user" + random number, or just "user" for now
                // In a real app we would ask for nick in a separate step
                let nick = format!("user-{}", rand::random::<u16>());
                // Assuming create_room handles "create if not exists" logic or just separate create/join
                // For now, let's just try to create and then connect
                app.update(ChatEvent::CreateRoom(room_name.clone())).await?;
                app.update(ChatEvent::ConnectToRoom(room_name, nick))
                    .await?;
            }
        }
        KeyCode::Char(c) => {
            app.model.input.push(c);
        }
        KeyCode::Backspace => {
            app.model.input.pop();
        }
        KeyCode::Esc => {
            app.update(ChatEvent::Disconnect).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_chat_view_input(
    key_event: KeyEvent,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    match key_event.code {
        KeyCode::Enter => {
            let payload = app.model.input.clone();
            if !payload.is_empty() {
                app.model.input.clear();
                if let Some(nick) = &app.model.user_nick {
                    let message = ChatMessage {
                        sender: nick.clone(),
                        payload,
                    };
                    app.update(ChatEvent::SendMessage(message)).await?;
                }
            }
        }
        KeyCode::Char(c) => {
            app.model.input.push(c);
        }
        KeyCode::Backspace => {
            app.model.input.pop();
        }
        KeyCode::Esc => {
            app.update(ChatEvent::ExitRoom).await?;
        }
        _ => {}
    }
    Ok(())
}
