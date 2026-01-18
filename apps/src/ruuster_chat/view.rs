use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, BorderType, Borders, Paragraph},
    Frame,
};

use crate::{app::App, model::RunningState};

pub fn render(app: &App, frame: &mut Frame) {
    match app.model.running_state {
        RunningState::StartView => render_start_view(app, frame),
        RunningState::RoomListView => render_room_list_view(app, frame),
        RunningState::ChatView => render_chat_view(app, frame),
        RunningState::Done => {}
    }
}

fn render_start_view(app: &App, frame: &mut Frame) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(60),
            ]
            .as_ref(),
        )
        .split(frame.area());

    let title = Paragraph::new("Ruuster Chat")
        .style(Style::default().fg(Color::Cyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .title("Welcome")
                .border_type(BorderType::Plain),
        );

    frame.render_widget(title, chunks[0]);

    let input = Paragraph::new(app.model.input.as_str())
        .style(Style::default().fg(Color::Yellow))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .title("Enter Server Address (default: http://127.0.0.1:50051)")
                .border_type(BorderType::Plain),
        );

    frame.render_widget(input, chunks[1]);
}

fn render_room_list_view(app: &App, frame: &mut Frame) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(60),
            ]
            .as_ref(),
        )
        .split(frame.area());

    let title = Paragraph::new("Room List")
        .style(Style::default().fg(Color::Cyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .title("Connected")
                .border_type(BorderType::Plain),
        );

    frame.render_widget(title, chunks[0]);

    // Simplified for now - just ask for room name to create/join
    let input = Paragraph::new(app.model.input.as_str())
        .style(Style::default().fg(Color::Yellow))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .title("Enter Room Name to Create/Join")
                .border_type(BorderType::Plain),
        );

    frame.render_widget(input, chunks[1]);
}

fn render_chat_view(app: &App, frame: &mut Frame) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Percentage(10),
                Constraint::Percentage(80),
                Constraint::Percentage(10),
            ]
            .as_ref(),
        )
        .split(frame.area());

    let room_info = format!(
        "Room: {} | User: {}",
        app.model.current_room.as_deref().unwrap_or("Unknown"),
        app.model.user_nick.as_deref().unwrap_or("Unknown")
    );

    let title = Paragraph::new(room_info)
        .style(Style::default().fg(Color::Cyan))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        );
    frame.render_widget(title, chunks[0]);

    let messages: Vec<String> = app
        .model
        .messages
        .iter()
        .map(|m| format!("{}: {}", m.sender, m.payload))
        .collect();

    let messages_text = messages.join("\n");
    let messages_widget = Paragraph::new(messages_text)
        .block(Block::default().borders(Borders::ALL).title("Messages"));
    frame.render_widget(messages_widget, chunks[1]);

    let input = Paragraph::new(app.model.input.as_str())
        .style(Style::default().fg(Color::Yellow))
        .block(Block::default().borders(Borders::ALL).title("Input"));
    frame.render_widget(input, chunks[2]);
}
