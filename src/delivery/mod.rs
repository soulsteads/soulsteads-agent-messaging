mod admission;
#[cfg(feature = "agent-network")]
pub mod agent_tools;
mod local;
mod types;

pub use local::{
    LocalClient, LocalRefusal, RecipientState, RecordStatus, ReplyHandle, ReplyOutcome,
    SendOutcome, SourceClass, VisibleRecord,
};
pub use types::{
    AgentId, ConversationKey, DeliveryContractError, DeliveryMode, MAX_AGENT_ID_BYTES,
    MAX_CONVERSATION_CONTEXT_BYTES, MAX_IDEMPOTENCY_KEY_BYTES, MAX_TEXT_BYTES, MessageKind,
    OutputClass, PeerOutput, PeerSend,
};
#[cfg(feature = "agent-network")]
pub use types::{DirectRefusal, DirectRequest, DirectResult};

pub(crate) use admission::AdmissionConfig;
#[cfg(feature = "agent-network")]
pub(crate) use local::DirectAdmission;
pub(crate) use local::{AdmittedOrigin, OwnerRegistry};
