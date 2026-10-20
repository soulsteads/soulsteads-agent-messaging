pub mod http;
#[cfg(feature = "platform-sdks")]
pub mod matrix;
#[cfg(feature = "platform-sdks")]
#[path = "matrix/attachment.rs"]
mod matrix_attachment;

// Matrix helpers remain available without the optional platform SDKs because
// runtime and ACP use their scoped broker and restart contracts directly.
#[path = "matrix/indicators.rs"]
pub mod matrix_indicators;
#[path = "matrix/permissions.rs"]
pub(crate) mod matrix_permissions;
#[path = "matrix/questions.rs"]
pub(crate) mod matrix_questions;
#[path = "matrix/restart.rs"]
pub mod matrix_restart;

#[cfg(feature = "platform-sdks")]
pub mod whatsapp;
