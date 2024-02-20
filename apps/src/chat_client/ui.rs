use ratatui::{
    layout::{self, Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::app::App;

fn render_user_name_input(app: &App, frame: &mut Frame) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(1), Constraint::Min(5)])
        .split(frame.size());

    let input_name_msg = Line::from(vec![
        Span::from("Enter your user name: "),
        Span::styled(
            format!("{}", app.user_name),
            Style::default().fg(Color::Green),
        ),
    ]);

    frame.render_widget(
        Paragraph::new(input_name_msg)
            .block(Block::new().borders(Borders::ALL).title("Ruuster chat"))
            .alignment(Alignment::Center),
        layout[1],
    );

    let esc_msg = Span::styled(
        "ESC - exit",
        Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
    );

    frame.render_widget(
        Paragraph::new(esc_msg).alignment(Alignment::Right),
        layout[0],
    );
}

fn render_chat(app: &App, frame: &mut Frame) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Length(1), // controls
            Constraint::Min(5),    // msgs
            Constraint::Length(3), //input
        ])
        .split(frame.size());

    let esc_msg = Span::styled(
        "ESC - exit",
        Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
    );

    frame.render_widget(
        Paragraph::new(esc_msg).alignment(Alignment::Right),
        layout[0],
    );

    frame.render_widget(Paragraph::, area)
}
