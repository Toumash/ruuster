use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use protos::ruuster_client::RuusterClient;
use tonic::transport::Channel;

use crate::{
    app::{App, AppState, ChatMessage},
    request::{listen_for_message, send_message, setup_user},
};

async fn update_user_name_input(
    client: &mut RuusterClient<Channel>,
    app: &mut App,
    key_event: KeyEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    match key_event.code {
        KeyCode::Esc => app.quit(),
        KeyCode::Char('c') | KeyCode::Char('C') => {
            if key_event.modifiers == KeyModifiers::CONTROL {
                app.quit()
            } else {
            }
        }
        KeyCode::Char(key) => {
            app.user_name.push(key);
        }
        KeyCode::Backspace => {
            app.user_name.pop();
        }
        KeyCode::Enter => {
            if app.validate_user_name() {
                setup_user(client, app).await?;
                app.state = AppState::Running;
            }
        }
        _ => {}
    };
    Ok(())
}

pub async fn update_message_list(
    client: &mut RuusterClient<Channel>,
    app: &mut App
) -> Result<(), Box<dyn std::error::Error>> {
    if app.state != AppState::Running {
        return Ok(());
    }

    listen_for_message(client, app).await?;

    Ok(())
}


async fn update_chat_input(
    client: &mut RuusterClient<Channel>,
    app: &mut App,
    key_event: KeyEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    match key_event.code {
        KeyCode::Esc => app.quit(),
        KeyCode::Char('c') => {
            if key_event.modifiers == KeyModifiers::CONTROL {
                app.quit()
            } else {
                app.input_message.push('c');
            }
        }
        KeyCode::Up => {
            app.scroll_up();
        }
        KeyCode::Down => {
            app.scroll_down();
        }
        KeyCode::Char(key) => {
            app.input_message.push(key);
        }
        KeyCode::Backspace => {
            app.input_message.pop();
        }
        KeyCode::Enter => {
            // send message to exchange
            send_message(
                client,
                ChatMessage {
                    author: app.user_name.clone(),
                    content: app.input_message.clone(),
                },
            ).await?;
            app.input_message.clear();
        }
        _ => {}
    };
    Ok(())
}

pub async fn dispatch_update(
    client: &mut RuusterClient<Channel>,
    app: &mut App,
    key_event: KeyEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    match app.state {
        AppState::InputName => update_user_name_input(client, app, key_event).await?,
        AppState::Running => update_chat_input(client, app, key_event).await?,
        AppState::Help => todo!(),
        AppState::Done => todo!(),
    }
    Ok(())
}
