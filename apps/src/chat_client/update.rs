use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::{App, AppState};

pub fn update_user_name_input(app: &mut App, key_event: KeyEvent) {
    match key_event.code {
        KeyCode::Esc => app.quit(),
        KeyCode::Char('c') | KeyCode::Char('C') => {
            if key_event.modifiers == KeyModifiers::CONTROL {
                app.quit()
            }
            else {
                
            }
        }
        KeyCode::Char(key) => {
            app.user_name.push(key);
        }
        KeyCode::Backspace => {
            app.user_name.pop();
        }
        KeyCode::Enter => {
            app.state = AppState::Running;
        }
        _ => {}
    };
}

pub fn update_chat_input(app: &mut App, key_event: KeyEvent) {
    todo!()
}