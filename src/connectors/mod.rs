pub mod http;
#[cfg(feature = "platform-sdks")]
pub mod matrix;
#[cfg(feature = "platform-sdks")]
mod matrix_attachment;
pub mod matrix_indicators;
pub(crate) mod matrix_permissions;
pub(crate) mod matrix_questions;
pub mod matrix_restart;
#[cfg(feature = "platform-sdks")]
pub mod whatsapp;
