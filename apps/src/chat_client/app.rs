#[derive(Debug, Default, PartialEq, Eq)]
pub enum AppState {
    #[default] 
    InputName,
    Running,
    Help,
    Done
}

#[derive(Debug)]
pub enum UserNameValidationError {
    NameTooShort
}

#[derive(Debug, Default)]
pub struct App {
    pub state: AppState,
    pub user_name: String,
    pub input_message: String,
    pub messages: Vec<String>
}

impl App {
    pub fn new() -> Self {
        let mut default = Self::default();
        default.messages = std::iter::repeat_with(|| utils::generate_random_string(20)).take(100).collect();
        default
    }

    pub fn quit(&mut self) {
        self.state = AppState::Done;
    }

    pub fn validate_user_name(&self, name: &String) -> bool {
        if name.len() > 0 {
            return true;
        }
        false
    }

    pub fn is_done(&self) -> bool {
        self.state == AppState::Done
    }
}