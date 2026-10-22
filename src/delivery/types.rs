use std::fmt;

/// A validation failure at the delivery-contract boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryContractError {
    InvalidAgentId(&'static str),
    InvalidConversationKey(&'static str),
    InvalidIdempotencyKey(&'static str),
    InvalidText(&'static str),
}

impl fmt::Display for DeliveryContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAgentId(reason) => write!(formatter, "invalid agent id: {reason}"),
            Self::InvalidConversationKey(reason) => {
                write!(formatter, "invalid conversation key: {reason}")
            }
            Self::InvalidIdempotencyKey(reason) => {
                write!(formatter, "invalid idempotency key: {reason}")
            }
            Self::InvalidText(reason) => write!(formatter, "invalid delivery text: {reason}"),
        }
    }
}

impl std::error::Error for DeliveryContractError {}

/// Maximum UTF-8 byte length of a public agent identifier.
pub const MAX_AGENT_ID_BYTES: usize = 255;
/// Maximum UTF-8 byte length of a platform-native conversation context.
pub const MAX_CONVERSATION_CONTEXT_BYTES: usize = 4096;
/// Maximum UTF-8 byte length of a caller-provided idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 255;
/// Maximum UTF-8 byte length of a peer input or typed output payload.
pub const MAX_TEXT_BYTES: usize = 64 * 1024;

/// A caller-supplied agent identifier validated at the public delivery boundary.
///
/// This syntax check does not authenticate a principal.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AgentId(String);

impl AgentId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for AgentId {
    type Error = DeliveryContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(DeliveryContractError::InvalidAgentId("must not be empty"));
        }
        if value.len() > MAX_AGENT_ID_BYTES {
            return Err(DeliveryContractError::InvalidAgentId("exceeds byte limit"));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(DeliveryContractError::InvalidAgentId(
                "contains a reserved character",
            ));
        }

        Ok(Self(value.to_owned()))
    }
}

/// A platform and platform-native conversation context.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConversationKey {
    platform: String,
    context: String,
}

impl ConversationKey {
    pub fn new(platform: &str, context: &str) -> Result<Self, DeliveryContractError> {
        AgentId::try_from(platform)?;
        if context.is_empty() {
            return Err(DeliveryContractError::InvalidConversationKey(
                "context must not be empty",
            ));
        }
        if context.len() > MAX_CONVERSATION_CONTEXT_BYTES {
            return Err(DeliveryContractError::InvalidConversationKey(
                "context exceeds byte limit",
            ));
        }
        if context.contains('\0') {
            return Err(DeliveryContractError::InvalidConversationKey(
                "context contains the legacy separator",
            ));
        }

        Ok(Self {
            platform: platform.to_owned(),
            context: context.to_owned(),
        })
    }

    pub fn from_legacy_owner_key(value: &str) -> Result<Self, DeliveryContractError> {
        let (platform, context) =
            value
                .split_once('\0')
                .ok_or(DeliveryContractError::InvalidConversationKey(
                    "missing platform separator",
                ))?;
        Self::new(platform, context)
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn context(&self) -> &str {
        &self.context
    }

    pub fn legacy_owner_key(&self) -> String {
        format!("{}\0{}", self.platform, self.context)
    }
}

/// Selects the delivery path without identifying an authenticated caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryMode {
    LegacyHuman,
    QueueOnly,
}

/// Describes the semantic role of a delivery payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    Request,
    Answer,
    Information,
}

/// Keeps peer responses distinct from conversation and operational outputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputClass {
    PeerAnswer,
    ConversationOutput,
    Receipt,
    Activity,
    Presence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundedText(String);

impl BoundedText {
    fn new(value: impl Into<String>) -> Result<Self, DeliveryContractError> {
        let value = value.into();
        if value.len() > MAX_TEXT_BYTES {
            return Err(DeliveryContractError::InvalidText(
                "exceeds 64 KiB byte limit",
            ));
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

/// Caller-provided peer input, intentionally limited to recipient, idempotency, and text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSend {
    recipient: AgentId,
    idempotency_key: String,
    text: BoundedText,
}

impl PeerSend {
    pub fn new(
        recipient: AgentId,
        idempotency_key: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, DeliveryContractError> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.is_empty() {
            return Err(DeliveryContractError::InvalidIdempotencyKey(
                "must not be empty",
            ));
        }
        if idempotency_key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(DeliveryContractError::InvalidIdempotencyKey(
                "exceeds byte limit",
            ));
        }
        if idempotency_key.contains('\0') {
            return Err(DeliveryContractError::InvalidIdempotencyKey(
                "contains a reserved separator",
            ));
        }
        Ok(Self {
            recipient,
            idempotency_key,
            text: BoundedText::new(text)?,
        })
    }

    pub fn recipient(&self) -> &AgentId {
        &self.recipient
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn text(&self) -> &str {
        self.text.as_str()
    }
}

/// A bounded in-process request to one exact configured agent owner.
///
/// This request has no durable acceptance or idempotency guarantee.
#[cfg(feature = "agent-network")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectRequest {
    recipient: AgentId,
    text: BoundedText,
}

#[cfg(feature = "agent-network")]
impl DirectRequest {
    pub fn new(recipient: AgentId, text: impl Into<String>) -> Result<Self, DeliveryContractError> {
        Ok(Self {
            recipient,
            text: BoundedText::new(text)?,
        })
    }

    pub fn recipient(&self) -> &AgentId {
        &self.recipient
    }

    pub fn text(&self) -> &str {
        self.text.as_str()
    }
}

/// The terminal result of an in-process direct request.
///
/// The correlation ID is runtime-generated and is not a durable receipt.
#[cfg(feature = "agent-network")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectResult {
    correlation_id: uuid::Uuid,
    text: String,
    stop_reason: Option<String>,
}

#[cfg(feature = "agent-network")]
impl DirectResult {
    pub(crate) fn new(
        correlation_id: uuid::Uuid,
        text: String,
        stop_reason: Option<String>,
    ) -> Self {
        Self {
            correlation_id,
            text,
            stop_reason,
        }
    }

    pub fn correlation_id(&self) -> uuid::Uuid {
        self.correlation_id
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn stop_reason(&self) -> Option<&str> {
        self.stop_reason.as_deref()
    }
}

/// A fail-closed direct-request refusal.
#[cfg(feature = "agent-network")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectRefusal {
    Denied,
    Unavailable,
    Busy,
    Uncertain,
}

/// Typed output that cannot be mistaken for peer input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerOutput {
    class: OutputClass,
    text: BoundedText,
}

impl PeerOutput {
    pub fn new(class: OutputClass, text: impl Into<String>) -> Result<Self, DeliveryContractError> {
        Ok(Self {
            class,
            text: BoundedText::new(text)?,
        })
    }

    pub fn class(&self) -> OutputClass {
        self.class
    }

    pub fn text(&self) -> &str {
        self.text.as_str()
    }
}
